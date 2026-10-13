"""Logging setup shared by all Dragonfly entry points."""

from __future__ import annotations

import logging


def setup_logging(verbose: bool = False) -> None:
    """Configure root logging.

    librdkafka's own log lines (routed through the ``rdkafka`` logger) are
    noisy during broker outages, and our code already reports the conditions
    that matter, so they are hidden unless ``verbose`` is set.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("rdkafka").setLevel(logging.DEBUG if verbose else logging.CRITICAL)
