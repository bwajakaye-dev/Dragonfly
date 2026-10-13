"""Tests for the switchboard drawing helpers."""

from __future__ import annotations

import numpy as np
import pytest

from common.render import card, compose_grid, draw_label, fit, grid_shape, parse_grid


@pytest.mark.parametrize(
    ("count", "expected"),
    [(0, (1, 1)), (1, (1, 1)), (2, (2, 1)), (3, (2, 2)), (4, (2, 2)), (5, (3, 2)), (9, (3, 3))],
)
def test_grid_shape_auto(count: int, expected: tuple[int, int]) -> None:
    assert grid_shape(count) == expected


def test_grid_shape_fixed_overrides_count() -> None:
    assert grid_shape(7, (3, 1)) == (3, 1)


def test_parse_grid() -> None:
    assert parse_grid("auto") is None
    assert parse_grid("3x2") == (3, 2)
    assert parse_grid("2X2") == (2, 2)
    for bad in ("", "3", "0x2", "10x1", "axb", "3x2x1"):
        with pytest.raises(ValueError):
            parse_grid(bad)


def test_compose_grid_places_tiles_and_leaves_gaps_black() -> None:
    tiles = [np.full((10, 20, 3), v, dtype=np.uint8) for v in (50, 100, 150)]
    out = compose_grid(tiles, cols=2, rows=2)
    assert out.shape == (20, 40, 3)
    assert out[5, 5, 0] == 50
    assert out[5, 25, 0] == 100
    assert out[15, 5, 0] == 150
    assert out[15, 25].sum() == 0  # empty fourth slot


def test_fit_landscape_fills_width_with_letterbox() -> None:
    src = np.full((1080, 1920, 3), 255, dtype=np.uint8)
    out = fit(src, 640, 480)
    assert out.shape == (480, 640, 3)
    assert out[0, 320].sum() == 0  # black bar on top
    assert out[240, 320].sum() > 0  # picture in the middle


def test_fit_portrait_is_pillarboxed() -> None:
    src = np.full((1920, 1080, 3), 255, dtype=np.uint8)
    out = fit(src, 1280, 720)
    assert out.shape == (720, 1280, 3)
    assert out[360, 0].sum() == 0  # black bar at the left edge
    assert out[360, 640].sum() > 0


def test_fit_returns_new_array() -> None:
    src = np.zeros((90, 160, 3), dtype=np.uint8)
    out = fit(src, 160, 90)
    out[:] = 255
    assert src.sum() == 0


def test_card_and_label_draw_without_error() -> None:
    img = card(320, 180, "NO SIGNAL", "cam1")
    assert img.shape == (180, 320, 3)
    draw_label(img, ["cam1 | a very long source name that overflows.mp4", "latency 12 ms"])
    draw_label(img, [])
