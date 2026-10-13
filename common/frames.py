"""Frame wire format: JPEG encode/decode, chunking, and Kafka header handling.

A frame travels as raw JPEG bytes in the Kafka message value. All metadata
lives in Kafka headers, so the payload is never base64-inflated or re-parsed.
Frames larger than ``MAX_CHUNK_BYTES`` are split into ``chunk_total`` messages
sharing the same ``(camera_id, seq)``; because they share the partition key
``camera_id`` they arrive in order on one partition.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field

import cv2
import numpy as np

from common.config import DEFAULT_JPEG_QUALITY, MAX_CHUNK_BYTES

# --- Header keys -------------------------------------------------------------

H_CAMERA_ID = "camera_id"
H_SEQ = "seq"
H_CAPTURE_TS_NS = "capture_ts_ns"
H_MEDIA_POS_MS = "media_pos_ms"
H_CHUNK_IDX = "chunk_idx"
H_CHUNK_TOTAL = "chunk_total"
H_SOURCE_NAME = "source_name"

_INT_KEYS = (H_SEQ, H_CAPTURE_TS_NS, H_MEDIA_POS_MS, H_CHUNK_IDX, H_CHUNK_TOTAL)
_STR_KEYS = (H_CAMERA_ID, H_SOURCE_NAME)

# A 4K JPEG at high quality is a few MB; this bound rejects absurd headers
# before they can allocate anything.
MAX_CHUNK_TOTAL = 64

HeaderList = list[tuple[str, bytes]]


class MalformedFrame(ValueError):
    """Raised when a message's headers or chunk layout are invalid."""


@dataclass(frozen=True)
class FrameMeta:
    """Metadata carried in the Kafka headers of one frame chunk."""

    camera_id: str
    seq: int
    capture_ts_ns: int
    media_pos_ms: int
    chunk_idx: int
    chunk_total: int
    source_name: str


# --- Headers -----------------------------------------------------------------


def build_headers(meta: FrameMeta) -> HeaderList:
    """Encode ``meta`` as a Kafka header list (UTF-8 strings, ASCII integers)."""
    return [
        (H_CAMERA_ID, meta.camera_id.encode()),
        (H_SEQ, str(meta.seq).encode()),
        (H_CAPTURE_TS_NS, str(meta.capture_ts_ns).encode()),
        (H_MEDIA_POS_MS, str(meta.media_pos_ms).encode()),
        (H_CHUNK_IDX, str(meta.chunk_idx).encode()),
        (H_CHUNK_TOTAL, str(meta.chunk_total).encode()),
        (H_SOURCE_NAME, meta.source_name.encode()),
    ]


def parse_headers(headers: Sequence[tuple[str, bytes | None]] | None) -> FrameMeta:
    """Decode and validate Kafka headers into a :class:`FrameMeta`.

    Raises:
        MalformedFrame: if a key is missing, a value is undecodable or
            non-numeric, or the chunk index/total are out of range.
    """
    if not headers:
        raise MalformedFrame("message has no headers")
    values: dict[str, bytes] = {}
    for key, value in headers:
        if value is not None:
            values[key] = value

    missing = [k for k in (*_STR_KEYS, *_INT_KEYS) if k not in values]
    if missing:
        raise MalformedFrame(f"missing headers: {', '.join(missing)}")

    try:
        strs = {k: values[k].decode("utf-8") for k in _STR_KEYS}
        ints = {k: int(values[k].decode("ascii")) for k in _INT_KEYS}
    except (UnicodeDecodeError, ValueError) as exc:
        raise MalformedFrame(f"bad header value: {exc}") from exc

    if not strs[H_CAMERA_ID]:
        raise MalformedFrame("empty camera_id")
    if ints[H_SEQ] < 0:
        raise MalformedFrame(f"negative seq {ints[H_SEQ]}")
    total = ints[H_CHUNK_TOTAL]
    if not 1 <= total <= MAX_CHUNK_TOTAL:
        raise MalformedFrame(f"chunk_total {total} out of range 1..{MAX_CHUNK_TOTAL}")
    if not 0 <= ints[H_CHUNK_IDX] < total:
        raise MalformedFrame(f"chunk_idx {ints[H_CHUNK_IDX]} out of range for total {total}")

    return FrameMeta(
        camera_id=strs[H_CAMERA_ID],
        seq=ints[H_SEQ],
        capture_ts_ns=ints[H_CAPTURE_TS_NS],
        media_pos_ms=ints[H_MEDIA_POS_MS],
        chunk_idx=ints[H_CHUNK_IDX],
        chunk_total=total,
        source_name=strs[H_SOURCE_NAME],
    )


# --- JPEG --------------------------------------------------------------------


def encode_jpeg(image: np.ndarray, quality: int = DEFAULT_JPEG_QUALITY) -> bytes:
    """Encode a BGR image to JPEG bytes.

    Raises:
        ValueError: if OpenCV cannot encode the image.
    """
    ok, buf = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, int(quality)])
    if not ok:
        raise ValueError("JPEG encoding failed")
    return buf.tobytes()


_REDUCED_FLAGS = {
    1: cv2.IMREAD_COLOR,
    2: cv2.IMREAD_REDUCED_COLOR_2,
    4: cv2.IMREAD_REDUCED_COLOR_4,
    8: cv2.IMREAD_REDUCED_COLOR_8,
}


def decode_jpeg(data: bytes, reduce: int = 1) -> np.ndarray | None:
    """Decode JPEG bytes to a BGR image, or return ``None`` if corrupt/empty.

    Args:
        reduce: 1, 2, 4 or 8. Decode directly at 1/reduce of the size, which
            is several times faster than decoding at full size and scaling
            down (the JPEG decoder skips the detail it would throw away).
    """
    if not data:
        return None
    arr = np.frombuffer(data, dtype=np.uint8)
    try:
        image = cv2.imdecode(arr, _REDUCED_FLAGS[reduce])
    except cv2.error:
        return None
    return image


def pick_reduction(src_w: int, src_h: int, max_w: int, max_h: int) -> int:
    """Largest JPEG decode reduction (1/2/4/8) that still fills a ``max_w x max_h`` box.

    A 3840x2160 frame shown in a 1280x720 window only needs 1280x720 pixels,
    so it can be decoded at 1/2 (1920x1080) instead of full size.
    """
    if src_w <= 0 or src_h <= 0:
        return 1
    scale = min(max_w / src_w, max_h / src_h)  # how much fit() will shrink it
    reduce = 1
    while reduce < 8 and scale * reduce * 2 <= 1.0:
        reduce *= 2
    return reduce


def resize_max_width(image: np.ndarray, max_width: int | None) -> np.ndarray:
    """Downscale ``image`` to ``max_width`` keeping aspect ratio; never upscale."""
    if not max_width:
        return image
    height, width = image.shape[:2]
    if width <= max_width:
        return image
    new_height = max(1, round(height * max_width / width))
    return cv2.resize(image, (max_width, new_height), interpolation=cv2.INTER_AREA)


# --- Chunking ----------------------------------------------------------------


def split_chunks(payload: bytes, max_chunk: int = MAX_CHUNK_BYTES) -> list[bytes]:
    """Split ``payload`` into pieces of at most ``max_chunk`` bytes.

    An empty payload yields a single empty chunk so every frame is at least
    one message.

    Raises:
        ValueError: if ``max_chunk`` is not positive or the payload needs more
            than ``MAX_CHUNK_TOTAL`` chunks.
    """
    if max_chunk <= 0:
        raise ValueError("max_chunk must be positive")
    if not payload:
        return [b""]
    chunks = [payload[i : i + max_chunk] for i in range(0, len(payload), max_chunk)]
    if len(chunks) > MAX_CHUNK_TOTAL:
        raise ValueError(f"payload needs {len(chunks)} chunks; limit is {MAX_CHUNK_TOTAL}")
    return chunks


@dataclass
class _Assembly:
    total: int
    started: float
    parts: dict[int, bytes] = field(default_factory=dict)


class Reassembler:
    """Collects chunks of multi-part frames and returns completed payloads.

    Memory is bounded two ways: incomplete assemblies older than
    ``max_age_s`` are evicted, and at most ``max_pending`` assemblies are kept
    (oldest dropped first). Not thread-safe; use from a single thread.
    """

    def __init__(
        self,
        max_age_s: float = 1.0,
        max_pending: int = 64,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_age_s = max_age_s
        self._max_pending = max_pending
        self._clock = clock
        self._pending: dict[tuple[str, int], _Assembly] = {}
        self.evicted = 0

    def __len__(self) -> int:
        return len(self._pending)

    def add(self, meta: FrameMeta, chunk: bytes) -> bytes | None:
        """Add one chunk; return the full payload when the frame is complete.

        Raises:
            MalformedFrame: if ``chunk_total`` disagrees with earlier chunks
                of the same frame (the partial assembly is discarded).
        """
        if meta.chunk_total == 1:
            return chunk

        self.evict_stale()
        key = (meta.camera_id, meta.seq)
        assembly = self._pending.get(key)
        if assembly is None:
            if len(self._pending) >= self._max_pending:
                oldest = min(self._pending, key=lambda k: self._pending[k].started)
                del self._pending[oldest]
                self.evicted += 1
            assembly = _Assembly(total=meta.chunk_total, started=self._clock())
            self._pending[key] = assembly
        elif assembly.total != meta.chunk_total:
            del self._pending[key]
            raise MalformedFrame(
                f"chunk_total changed from {assembly.total} to {meta.chunk_total} "
                f"for {meta.camera_id} seq {meta.seq}"
            )

        assembly.parts[meta.chunk_idx] = chunk
        if len(assembly.parts) < assembly.total:
            return None
        del self._pending[key]
        return b"".join(assembly.parts[i] for i in range(assembly.total))

    def evict_stale(self) -> int:
        """Drop assemblies older than ``max_age_s``; return how many were dropped."""
        cutoff = self._clock() - self._max_age_s
        stale = [k for k, a in self._pending.items() if a.started < cutoff]
        for key in stale:
            del self._pending[key]
        self.evicted += len(stale)
        return len(stale)


def frame_messages(
    payload: bytes,
    camera_id: str,
    seq: int,
    capture_ts_ns: int,
    media_pos_ms: int,
    source_name: str,
    max_chunk: int = MAX_CHUNK_BYTES,
) -> Iterable[tuple[bytes, HeaderList]]:
    """Yield ``(value, headers)`` for each Kafka message making up one frame."""
    chunks = split_chunks(payload, max_chunk)
    for idx, chunk in enumerate(chunks):
        meta = FrameMeta(
            camera_id=camera_id,
            seq=seq,
            capture_ts_ns=capture_ts_ns,
            media_pos_ms=media_pos_ms,
            chunk_idx=idx,
            chunk_total=len(chunks),
            source_name=source_name,
        )
        yield chunk, build_headers(meta)
