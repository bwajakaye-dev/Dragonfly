"""Drawing helpers for the switchboard (pure functions, no windows)."""

from __future__ import annotations

import math
from collections.abc import Sequence

import cv2
import numpy as np

FONT = cv2.FONT_HERSHEY_SIMPLEX
WHITE = (255, 255, 255)
GREY = (160, 160, 160)
RED = (0, 0, 230)
BLACK = (0, 0, 0)


def canvas(width: int, height: int, color: tuple[int, int, int] = BLACK) -> np.ndarray:
    """Return a new solid-color BGR image."""
    img = np.empty((height, width, 3), dtype=np.uint8)
    img[:] = color
    return img


def fit(image: np.ndarray, width: int, height: int) -> np.ndarray:
    """Scale ``image`` to fit ``width`` x ``height`` keeping aspect ratio.

    The result is always exactly ``width`` x ``height``; unused space is black
    (letterbox for wide sources, pillarbox for portrait ones). A new array is
    returned, so the caller may draw on it freely.
    """
    src_h, src_w = image.shape[:2]
    scale = min(width / src_w, height / src_h)
    new_w = max(1, round(src_w * scale))
    new_h = max(1, round(src_h * scale))
    interp = cv2.INTER_AREA if scale < 1 else cv2.INTER_LINEAR
    resized = cv2.resize(image, (new_w, new_h), interpolation=interp)
    out = canvas(width, height)
    x0 = (width - new_w) // 2
    y0 = (height - new_h) // 2
    out[y0 : y0 + new_h, x0 : x0 + new_w] = resized
    return out


def draw_label(img: np.ndarray, lines: Sequence[str], scale: float = 0.6) -> None:
    """Draw ``lines`` in the top-left corner over a translucent dark box."""
    if not lines:
        return
    thickness = 1 if scale < 0.8 else 2
    pad = 8
    sizes = [cv2.getTextSize(line, FONT, scale, thickness)[0] for line in lines]
    line_h = max(h for _, h in sizes) + pad
    box_w = min(img.shape[1], max(w for w, _ in sizes) + 2 * pad)
    box_h = min(img.shape[0], line_h * len(lines) + pad)
    region = img[:box_h, :box_w]
    region[:] = (region * 0.4).astype(np.uint8)
    for i, line in enumerate(lines):
        y = pad + line_h * (i + 1) - pad // 2
        color = WHITE if i == 0 else GREY
        cv2.putText(img, line, (pad, y), FONT, scale, color, thickness, cv2.LINE_AA)


def draw_centered(
    img: np.ndarray, text: str, y_frac: float = 0.5, scale: float = 1.0, color=WHITE
) -> None:
    """Draw ``text`` horizontally centered at ``y_frac`` of the image height."""
    thickness = max(1, round(scale * 2))
    (w, h), _ = cv2.getTextSize(text, FONT, scale, thickness)
    x = max(0, (img.shape[1] - w) // 2)
    y = int(img.shape[0] * y_frac + h / 2)
    cv2.putText(img, text, (x, y), FONT, scale, color, thickness, cv2.LINE_AA)


def card(width: int, height: int, title: str, subtitle: str = "") -> np.ndarray:
    """A dark placeholder tile, e.g. "WAITING FOR FEEDS" or "NO SIGNAL"."""
    img = canvas(width, height, (40, 40, 40))
    scale = max(0.5, width / 900)
    draw_centered(img, title, 0.45, scale)
    if subtitle:
        draw_centered(img, subtitle, 0.6, scale * 0.6, GREY)
    return img


def parse_grid(spec: str) -> tuple[int, int] | None:
    """Parse a ``--grid`` value: ``"auto"`` -> None, ``"3x2"`` -> (3 cols, 2 rows).

    Raises:
        ValueError: if ``spec`` is neither ``auto`` nor ``<cols>x<rows>``
            with both between 1 and 9.
    """
    if spec.strip().lower() == "auto":
        return None
    try:
        cols_s, rows_s = spec.lower().split("x")
        cols, rows = int(cols_s), int(rows_s)
    except ValueError as exc:
        raise ValueError(f"grid must be 'auto' or COLSxROWS like 3x2, got {spec!r}") from exc
    if not (1 <= cols <= 9 and 1 <= rows <= 9):
        raise ValueError(f"grid dimensions must be 1-9, got {spec!r}")
    return cols, rows


def grid_shape(count: int, fixed: tuple[int, int] | None = None) -> tuple[int, int]:
    """Columns and rows for ``count`` tiles: ``fixed`` if given, else near-square."""
    if fixed is not None:
        return fixed
    count = max(1, count)
    cols = math.ceil(math.sqrt(count))
    rows = math.ceil(count / cols)
    return cols, rows


def compose_grid(tiles: Sequence[np.ndarray], cols: int, rows: int) -> np.ndarray:
    """Place equal-sized tiles row by row; missing tiles are left black.

    Tiles beyond ``cols * rows`` are not drawn.
    """
    tile_h, tile_w = tiles[0].shape[:2]
    out = canvas(tile_w * cols, tile_h * rows)
    for i, tile in enumerate(tiles[: cols * rows]):
        r, c = divmod(i, cols)
        out[r * tile_h : (r + 1) * tile_h, c * tile_w : (c + 1) * tile_w] = tile
    return out


def draw_footer(img: np.ndarray, left: str, right: str, scale: float = 0.5) -> None:
    """Draw status text on a footer strip: ``left`` aligned left, ``right`` aligned right."""
    y = (img.shape[0] + cv2.getTextSize(left or right, FONT, scale, 1)[0][1]) // 2
    cv2.putText(img, left, (10, y), FONT, scale, WHITE, 1, cv2.LINE_AA)
    (w, _), _ = cv2.getTextSize(right, FONT, scale, 1)
    cv2.putText(img, right, (img.shape[1] - w - 10, y), FONT, scale, GREY, 1, cv2.LINE_AA)


def draw_banner(img: np.ndarray, text: str) -> None:
    """Draw a red warning strip across the top of ``img`` (in place)."""
    height = max(28, img.shape[0] // 14)
    img[:height] = RED
    scale = height / 45
    thickness = max(1, round(scale * 2))
    (w, h), _ = cv2.getTextSize(text, FONT, scale, thickness)
    x = max(4, (img.shape[1] - w) // 2)
    cv2.putText(img, text, (x, (height + h) // 2), FONT, scale, WHITE, thickness, cv2.LINE_AA)


def draw_border(img: np.ndarray, color: tuple[int, int, int], thickness: int = 6) -> None:
    """Draw a rectangle just inside the image edge."""
    h, w = img.shape[:2]
    half = thickness // 2
    cv2.rectangle(img, (half, half), (w - 1 - half, h - 1 - half), color, thickness)
