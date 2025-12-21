import json
from collections.abc import Callable, Iterable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from ray.data import Dataset, Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor


class KafkaDatasink(Datasink):
    """
    Ray Data sink for writing to Apache Kafka topics using confluent-kafka.

    Writes blocks of data to Kafka with configurable serialization
    and producer settings.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        key_field: str | None = None,
        value_serializer: str = "json",
        producer_config: dict[str, Any] | None = None,
        batch_size: int = 100,
        delivery_callback: Callable | None = None,
    ):
        """
        Initialize Kafka sink.

        Args:
            topic: Kafka topic name
            bootstrap_servers: Comma-separated Kafka broker addresses (e.g., 'localhost:9092')
            key_field: Optional field name to use as message key
            value_serializer: Serialization format ('json', 'string', or 'bytes')
            producer_config: Additional Kafka producer configuration (librdkafka format)
            batch_size: Number of records to batch before polling
            delivery_callback: Optional callback for delivery reports
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.key_field = key_field
        self.value_serializer = value_serializer
        self.producer_config = producer_config or {}
        self.batch_size = batch_size
        self.delivery_callback = delivery_callback

    def _row_to_dict(self, row: Any) -> Any:
        """Convert row to dict if it's an ArrowRow, otherwise return as-is."""
        # Handle ArrowRow objects from Ray Data
        if hasattr(row, "as_pydict"):
            return row.as_pydict()
        elif hasattr(row, "__dict__") and hasattr(row, "_fields"):
            # Handle named tuples or similar
            return dict(row._asdict()) if hasattr(row, "_asdict") else row
        else:
            return row

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value based on configured format."""
        # Convert ArrowRow to dict first
        value = self._row_to_dict(value)

        if self.value_serializer == "json":
            return json.dumps(value).encode("utf-8")
        elif self.value_serializer == "string":
            return str(value).encode("utf-8")
        else:  # bytes
            return value if isinstance(value, bytes) else str(value).encode("utf-8")

    def _extract_key(self, row: Any) -> bytes | None:
        """Extract and encode message key from row."""
        # Convert ArrowRow to dict first
        row_dict = self._row_to_dict(row)

        key = None
        if self.key_field and isinstance(row_dict, dict):
            key_value = row_dict.get(self.key_field)
            if key_value is not None:
                key = str(key_value).encode("utf-8")
        return key

    def _default_delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Default delivery report callback."""
        if err is not None:
            raise KafkaException(f"Message delivery failed: {err}")

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        """
        Write blocks of data to Kafka.

        Args:
            blocks: Iterable of Ray data blocks
            ctx: Ray data context

        Returns:
            Write statistics (total records written)
        """
        # Create producer with config
        config = {"bootstrap.servers": self.bootstrap_servers, **self.producer_config}

        producer = Producer(config)
        total_records = 0
        batch_count = 0
        failed_messages = 0

        # Use provided callback or default
        callback = self.delivery_callback or self._default_delivery_callback

        try:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)

                # Iterate through rows in block
                for row in block_accessor.iter_rows(public_row_format=False):
                    # Extract key if specified
                    key = self._extract_key(row)

                    # Serialize value
                    value = self._serialize_value(row)

                    # Produce to Kafka
                    try:
                        producer.produce(topic=self.topic, value=value, key=key, callback=callback)
                        total_records += 1
                        batch_count += 1

                    except BufferError:
                        # Queue is full, wait for messages to be delivered
                        producer.poll(1.0)
                        # Retry
                        producer.produce(topic=self.topic, value=value, key=key, callback=callback)
                        total_records += 1
                        batch_count += 1

                    # Poll periodically for delivery reports
                    if batch_count >= self.batch_size:
                        producer.poll(0)
                        batch_count = 0

            # Final flush to ensure all messages are sent
            remaining = producer.flush(timeout=30.0)
            if remaining > 0:
                failed_messages = remaining

        except KafkaException as e:
            raise RuntimeError(f"Failed to write to Kafka: {e}") from e
        finally:
            # Flush any remaining messages
            producer.flush()

        return {"total_records": total_records, "failed_messages": failed_messages}


def write_kafka(
    dataset: Dataset,
    topic: str,
    bootstrap_servers: str,
    key_field: str | None = None,
    value_serializer: str = "json",
    producer_config: dict[str, Any] | None = None,
    batch_size: int = 100,
    delivery_callback: Callable | None = None,
) -> Any:
    """
    Convenience method to write Ray Dataset to Kafka.

    Args:
        dataset: Ray Dataset to write
        topic: Kafka topic name
        bootstrap_servers: Comma-separated Kafka broker addresses
        key_field: Optional field name to use as message key
        value_serializer: Serialization format ('json', 'string', or 'bytes')
        producer_config: Additional Kafka producer configuration (librdkafka format)
        batch_size: Number of records to batch before polling
        delivery_callback: Optional callback for delivery reports

    Returns:
        Write statistics

    Example:
        >>> ds = ray.data.range(100)
        >>> write_kafka(ds, "my-topic", "localhost:9092")
    """
    sink = KafkaDatasink(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        key_field=key_field,
        value_serializer=value_serializer,
        producer_config=producer_config,
        batch_size=batch_size,
        delivery_callback=delivery_callback,
    )
    return dataset.write_datasink(sink)
