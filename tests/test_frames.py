"""Tests for the frame wire format: headers, chunking, reassembly, JPEG."""

from __future__ import annotations

import os

import numpy as np
import pytest

from common.frames import (
    MAX_CHUNK_TOTAL,
    FrameMeta,
    MalformedFrame,
    Reassembler,
    build_headers,
    decode_jpeg,
    encode_jpeg,
    frame_messages,
    parse_headers,
    pick_reduction,
    resize_max_width,
    split_chunks,
)


def make_meta(**overrides: object) -> FrameMeta:
    fields: dict[str, object] = {
        "camera_id": "cam1",
        "seq": 42,
        "capture_ts_ns": 1_700_000_000_123_456_789,
        "media_pos_ms": 12_345,
        "chunk_idx": 0,
        "chunk_total": 1,
        "source_name": "court_left.mp4",
    }
    fields.update(overrides)
    return FrameMeta(**fields)  # type: ignore[arg-type]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


# --- Headers -----------------------------------------------------------------


def test_header_round_trip() -> None:
    meta = make_meta(chunk_idx=2, chunk_total=3, source_name="tribüne.mp4")
    assert parse_headers(build_headers(meta)) == meta


def test_header_order_does_not_matter() -> None:
    meta = make_meta()
    assert parse_headers(list(reversed(build_headers(meta)))) == meta


@pytest.mark.parametrize("headers", [None, []])
def test_no_headers_rejected(headers: list[tuple[str, bytes]] | None) -> None:
    with pytest.raises(MalformedFrame):
        parse_headers(headers)


def test_missing_key_rejected() -> None:
    headers = [h for h in build_headers(make_meta()) if h[0] != "seq"]
    with pytest.raises(MalformedFrame, match="seq"):
        parse_headers(headers)


def test_none_value_treated_as_missing() -> None:
    headers = [(k, None if k == "camera_id" else v) for k, v in build_headers(make_meta())]
    with pytest.raises(MalformedFrame, match="camera_id"):
        parse_headers(headers)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("seq", b"not-a-number"),
        ("seq", b"-1"),
        ("capture_ts_ns", b"\xff\xfe"),
        ("camera_id", b"\xff\xfe"),
        ("camera_id", b""),
        ("chunk_total", b"0"),
        ("chunk_total", str(MAX_CHUNK_TOTAL + 1).encode()),
        ("chunk_idx", b"1"),  # idx must be < total (1)
        ("chunk_idx", b"-1"),
    ],
)
def test_bad_header_values_rejected(key: str, value: bytes) -> None:
    headers = [(k, value if k == key else v) for k, v in build_headers(make_meta())]
    with pytest.raises(MalformedFrame):
        parse_headers(headers)


# --- Chunking ----------------------------------------------------------------


def test_small_payload_is_one_chunk() -> None:
    assert split_chunks(b"abc", max_chunk=10) == [b"abc"]


def test_empty_payload_is_one_empty_chunk() -> None:
    assert split_chunks(b"") == [b""]


def test_chunk_boundaries() -> None:
    chunks = split_chunks(b"x" * 25, max_chunk=10)
    assert [len(c) for c in chunks] == [10, 10, 5]


def test_too_many_chunks_rejected() -> None:
    with pytest.raises(ValueError):
        split_chunks(b"x" * (MAX_CHUNK_TOTAL + 1), max_chunk=1)


def test_chunk_round_trip_large_payload() -> None:
    payload = os.urandom(3_000_000)  # ~ a 4K JPEG
    reassembler = Reassembler()
    result = None
    messages = list(frame_messages(payload, "cam4k", 7, 1, 0, "uhd.mp4"))
    assert len(messages) == 4  # 900 KB chunks
    for value, headers in messages:
        assert len(value) <= 900_000
        result = reassembler.add(parse_headers(headers), value)
    assert result == payload
    assert len(reassembler) == 0


def test_reassembly_out_of_order_and_interleaved() -> None:
    a, b = os.urandom(25), os.urandom(25)
    msgs_a = list(frame_messages(a, "camA", 1, 0, 0, "a", max_chunk=10))
    msgs_b = list(frame_messages(b, "camB", 1, 0, 0, "b", max_chunk=10))
    reassembler = Reassembler()
    order = [msgs_a[2], msgs_b[0], msgs_a[0], msgs_b[1], msgs_a[1], msgs_b[2]]
    done = [r for v, h in order if (r := reassembler.add(parse_headers(h), v)) is not None]
    assert done == [a, b]


def test_single_chunk_bypasses_buffer() -> None:
    reassembler = Reassembler()
    assert reassembler.add(make_meta(), b"jpeg") == b"jpeg"
    assert len(reassembler) == 0


def test_incomplete_assembly_evicted_after_max_age() -> None:
    clock = FakeClock()
    reassembler = Reassembler(max_age_s=1.0, clock=clock)
    assert reassembler.add(make_meta(chunk_total=2), b"half") is None
    assert len(reassembler) == 1
    clock.now = 1.5
    assert reassembler.evict_stale() == 1
    assert len(reassembler) == 0
    assert reassembler.evicted == 1


def test_pending_assemblies_are_capped() -> None:
    reassembler = Reassembler(max_pending=3)
    for seq in range(10):
        reassembler.add(make_meta(seq=seq, chunk_total=2), b"part")
    assert len(reassembler) == 3
    assert reassembler.evicted == 7


def test_inconsistent_chunk_total_rejected() -> None:
    reassembler = Reassembler()
    reassembler.add(make_meta(chunk_total=3), b"a")
    with pytest.raises(MalformedFrame):
        reassembler.add(make_meta(chunk_idx=1, chunk_total=2), b"b")
    assert len(reassembler) == 0


# --- JPEG --------------------------------------------------------------------


def test_jpeg_round_trip() -> None:
    image = np.zeros((72, 128, 3), dtype=np.uint8)
    image[:, :64] = (0, 0, 255)
    decoded = decode_jpeg(encode_jpeg(image, quality=90))
    assert decoded is not None
    assert decoded.shape == image.shape
    assert np.abs(decoded.astype(int) - image.astype(int)).mean() < 5


@pytest.mark.parametrize("data", [b"", b"not a jpeg at all", b"\xff\xd8\xff" + b"\x00" * 100])
def test_corrupt_jpeg_decodes_to_none(data: bytes) -> None:
    assert decode_jpeg(data) is None


@pytest.mark.parametrize(
    ("src", "box", "expected"),
    [
        ((1280, 720), (1280, 720), 1),  # exact fit: full decode
        ((1920, 1080), (1280, 720), 1),  # 1/2 would be 960 wide: too small
        ((3840, 2160), (1280, 720), 2),  # 1920 wide still covers 1280
        ((3840, 2160), (640, 360), 4),
        ((1080, 1920), (1280, 720), 2),  # portrait: height-limited (720 of 1920)
        ((7680, 4320), (320, 180), 8),  # capped at 8
        ((0, 0), (1280, 720), 1),
    ],
)
def test_pick_reduction(src: tuple[int, int], box: tuple[int, int], expected: int) -> None:
    assert pick_reduction(*src, *box) == expected


def test_reduced_decode_shrinks_image() -> None:
    image = np.zeros((720, 1280, 3), dtype=np.uint8)
    data = encode_jpeg(image)
    assert decode_jpeg(data, reduce=4).shape == (180, 320, 3)  # type: ignore[union-attr]


def test_resize_max_width_keeps_aspect_and_never_upscales() -> None:
    image = np.zeros((1080, 1920, 3), dtype=np.uint8)
    assert resize_max_width(image, 1280).shape == (720, 1280, 3)
    assert resize_max_width(image, 4000) is image
    assert resize_max_width(image, None) is image
