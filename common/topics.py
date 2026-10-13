"""Idempotent topic creation through the Kafka AdminClient."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import TOPIC_SPECS, TopicSpec

log = logging.getLogger(__name__)


def check_brokers(admin: AdminClient, timeout_s: float = 5.0) -> int:
    """Request cluster metadata and return the number of live brokers.

    Raises:
        KafkaException: if no broker answers within ``timeout_s``.
    """
    metadata = admin.list_topics(timeout=timeout_s)
    return len(metadata.brokers)


def ensure_topics(
    admin: AdminClient,
    replication_factor: int,
    min_insync_replicas: int,
    specs: Iterable[TopicSpec] = TOPIC_SPECS,
    timeout_s: float = 15.0,
) -> dict[str, str]:
    """Create every topic in ``specs`` that does not already exist.

    Existing topics are left untouched, so running this repeatedly is safe.

    Returns:
        Mapping of topic name to ``"created"`` or ``"exists"``.

    Raises:
        KafkaException: for any failure other than "topic already exists".
    """
    new_topics = [
        NewTopic(
            spec.name,
            num_partitions=spec.partitions,
            replication_factor=replication_factor,
            config={**spec.config, "min.insync.replicas": str(min_insync_replicas)},
        )
        for spec in specs
    ]
    futures = admin.create_topics(new_topics, request_timeout=timeout_s)

    results: dict[str, str] = {}
    for name, future in futures.items():
        try:
            future.result()
            results[name] = "created"
            log.debug("created topic %s", name)
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                results[name] = "exists"
                log.debug("topic %s already exists", name)
            else:
                raise
    return results
