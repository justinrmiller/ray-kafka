"""Unit tests for Kafka datasink with mocked dependencies."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from src.kafka_datasink import KafkaDatasink, write_kafka


class TestKafkaDatasinkInit:
    """Tests for datasink initialization."""

    def test_init_with_minimal_config(self):
        """Test datasink initialization with minimal configuration."""
        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
        )

        assert sink.topic == "test-topic"
        assert sink.bootstrap_servers == "localhost:9092"
        assert sink.key_field is None
        assert sink.value_serializer == "json"
        assert sink.batch_size == 100
        assert sink.delivery_callback is None

    def test_init_with_full_config(self):
        """Test datasink initialization with all configuration options."""
        callback = Mock()
        producer_config = {"acks": "all", "retries": 5}

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_field="id",
            value_serializer="string",
            producer_config=producer_config,
            batch_size=50,
            delivery_callback=callback,
        )

        assert sink.topic == "test-topic"
        assert sink.bootstrap_servers == "localhost:9092"
        assert sink.key_field == "id"
        assert sink.value_serializer == "string"
        assert sink.batch_size == 50
        assert sink.delivery_callback == callback
        assert sink.producer_config == producer_config


class TestRowConversion:
    """Tests for row to dict conversion."""

    def test_row_to_dict_with_dict(self):
        """Test that regular dicts pass through unchanged."""
        sink = KafkaDatasink("test", "localhost:9092")

        test_dict = {"id": 1, "value": "foo"}
        result = sink._row_to_dict(test_dict)

        assert result == test_dict

    def test_row_to_dict_with_arrow_row(self):
        """Test that ArrowRow objects are converted to dicts."""
        sink = KafkaDatasink("test", "localhost:9092")

        # Mock ArrowRow
        mock_arrow_row = Mock()
        mock_arrow_row.as_pydict.return_value = {"id": 1, "value": "foo"}

        result = sink._row_to_dict(mock_arrow_row)

        assert result == {"id": 1, "value": "foo"}
        mock_arrow_row.as_pydict.assert_called_once()

    def test_row_to_dict_with_primitive(self):
        """Test that primitives pass through unchanged."""
        sink = KafkaDatasink("test", "localhost:9092")

        assert sink._row_to_dict(42) == 42
        assert sink._row_to_dict("string") == "string"


class TestSerializers:
    """Tests for value serialization."""

    def test_json_serializer(self):
        """Test JSON serializer produces valid JSON bytes."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="json",
        )

        test_data = {"foo": "bar", "num": 42, "nested": {"key": "value"}}
        serialized = sink._serialize_value(test_data)

        assert isinstance(serialized, bytes)
        deserialized = json.loads(serialized.decode("utf-8"))
        assert deserialized == test_data

    def test_json_serializer_with_arrow_row(self):
        """Test JSON serializer handles ArrowRow objects."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="json",
        )

        # Mock ArrowRow
        mock_arrow_row = Mock()
        mock_arrow_row.as_pydict.return_value = {"id": 1, "value": "foo"}

        serialized = sink._serialize_value(mock_arrow_row)

        assert isinstance(serialized, bytes)
        deserialized = json.loads(serialized.decode("utf-8"))
        assert deserialized == {"id": 1, "value": "foo"}

    def test_string_serializer(self):
        """Test string serializer converts to UTF-8 bytes."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="string",
        )

        test_str = "hello world"
        serialized = sink._serialize_value(test_str)

        assert isinstance(serialized, bytes)
        assert serialized.decode("utf-8") == test_str

    def test_string_serializer_with_dict(self):
        """Test string serializer with dict input."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="string",
        )

        test_dict = {"key": "value"}
        serialized = sink._serialize_value(test_dict)

        assert isinstance(serialized, bytes)
        assert "key" in serialized.decode("utf-8")

    def test_bytes_serializer_with_bytes(self):
        """Test bytes serializer handles bytes input."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="bytes",
        )

        test_bytes = b"raw bytes"
        result = sink._serialize_value(test_bytes)

        assert result == test_bytes

    def test_bytes_serializer_with_string(self):
        """Test bytes serializer converts non-bytes to bytes."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="bytes",
        )

        test_str = "convert me"
        result = sink._serialize_value(test_str)

        assert isinstance(result, bytes)
        assert result.decode("utf-8") == test_str


class TestKeyExtraction:
    """Tests for message key extraction."""

    def test_extract_key_with_dict(self):
        """Test key extraction from dict row."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="id",
        )

        row = {"id": 123, "value": "foo"}
        key = sink._extract_key(row)

        assert key == b"123"

    def test_extract_key_with_arrow_row(self):
        """Test key extraction from ArrowRow."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="user_id",
        )

        # Mock ArrowRow
        mock_arrow_row = Mock()
        mock_arrow_row.as_pydict.return_value = {"user_id": "alice", "action": "login"}

        key = sink._extract_key(mock_arrow_row)

        assert key == b"alice"

    def test_extract_key_missing_field(self):
        """Test key extraction when field doesn't exist."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="missing_field",
        )

        row = {"other_field": "value"}
        key = sink._extract_key(row)

        assert key is None

    def test_extract_key_non_dict(self):
        """Test key extraction from non-dict row."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="id",
        )

        row = 42
        key = sink._extract_key(row)

        assert key is None

    def test_extract_key_none_value(self):
        """Test key extraction when field value is None."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="id",
        )

        row = {"id": None, "value": "foo"}
        key = sink._extract_key(row)

        assert key is None


class TestWriteMethod:
    """Tests for the write method with mocked producer."""

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_basic(self, mock_accessor_class, mock_producer_class):
        """Test basic write functionality."""
        # Setup mocks
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [
            {"id": 1, "value": "foo"},
            {"id": 2, "value": "bar"},
        ]
        mock_accessor_class.for_block.return_value = mock_accessor

        # Create sink and write
        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
        )

        mock_blocks = [Mock()]
        mock_ctx = Mock()

        result = sink.write(mock_blocks, mock_ctx)

        # Verify
        assert mock_producer.produce.call_count == 2
        assert mock_producer.flush.called
        assert result["total_records"] == 2
        assert result["failed_messages"] == 0

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_with_keys(self, mock_accessor_class, mock_producer_class):
        """Test write with key extraction."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [
            {"id": 1, "value": "foo"},
            {"id": 2, "value": "bar"},
        ]
        mock_accessor_class.for_block.return_value = mock_accessor

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_field="id",
        )

        _ = sink.write([Mock()], Mock())

        # Verify keys were passed
        for call_args in mock_producer.produce.call_args_list:
            assert call_args[1]["key"] is not None
            assert isinstance(call_args[1]["key"], bytes)

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_buffer_error_retry(self, mock_accessor_class, mock_producer_class):
        """Test BufferError triggers retry."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer.produce.side_effect = [
            BufferError("Queue full"),
            None,  # Retry succeeds
        ]
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [{"id": 1}]
        mock_accessor_class.for_block.return_value = mock_accessor

        sink = KafkaDatasink("test-topic", "localhost:9092")

        _ = sink.write([Mock()], Mock())

        # Should have called poll and retried
        assert mock_producer.poll.called
        assert mock_producer.produce.call_count == 2

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_kafka_exception(self, mock_accessor_class, mock_producer_class):
        """Test KafkaException handling."""
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [{"id": 1}]
        mock_accessor_class.for_block.return_value = mock_accessor

        sink = KafkaDatasink("test-topic", "localhost:9092")

        with pytest.raises(RuntimeError, match="Failed to write to Kafka"):
            sink.write([Mock()], Mock())

        # Verify cleanup
        assert mock_producer.flush.called

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_batching(self, mock_accessor_class, mock_producer_class):
        """Test that batching triggers polling."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        # Create 150 rows to trigger batch polling at 100
        rows = [{"id": i} for i in range(150)]
        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = rows
        mock_accessor_class.for_block.return_value = mock_accessor

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            batch_size=100,
        )

        result = sink.write([Mock()], Mock())

        # Should have polled at 100 records
        assert mock_producer.poll.call_count >= 1
        assert result["total_records"] == 150

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_custom_callback(self, mock_accessor_class, mock_producer_class):
        """Test custom delivery callback is used."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [{"id": 1}]
        mock_accessor_class.for_block.return_value = mock_accessor

        callback = Mock()
        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            delivery_callback=callback,
        )

        sink.write([Mock()], Mock())

        # Verify callback was passed
        call_args = mock_producer.produce.call_args[1]
        assert call_args["callback"] == callback

    @patch("src.kafka_datasink.Producer")
    @patch("src.kafka_datasink.BlockAccessor")
    def test_write_producer_config(self, mock_accessor_class, mock_producer_class):
        """Test producer config is passed correctly."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        mock_accessor = MagicMock()
        mock_accessor.iter_rows.return_value = [{"id": 1}]
        mock_accessor_class.for_block.return_value = mock_accessor

        custom_config = {"acks": "all", "retries": 5}
        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            producer_config=custom_config,
        )

        sink.write([Mock()], Mock())

        # Verify config
        config = mock_producer_class.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["acks"] == "all"
        assert config["retries"] == 5


class TestDeliveryCallback:
    """Tests for delivery callback."""

    def test_default_delivery_callback_success(self):
        """Test default callback with successful delivery."""
        sink = KafkaDatasink("test", "localhost:9092")

        # Should not raise
        sink._default_delivery_callback(None, Mock())

    def test_default_delivery_callback_error(self):
        """Test default callback with delivery error."""
        sink = KafkaDatasink("test", "localhost:9092")

        error = KafkaError(KafkaError._MSG_TIMED_OUT)

        with pytest.raises(KafkaException, match="Message delivery failed"):
            sink._default_delivery_callback(error, Mock())


class TestWriteKafkaHelper:
    """Tests for the write_kafka convenience function."""

    def test_write_kafka_creates_sink(self):
        """Test write_kafka creates datasink with correct params."""
        mock_dataset = Mock()

        write_kafka(
            dataset=mock_dataset,
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_field="id",
            value_serializer="string",
            batch_size=200,
        )

        # Verify dataset.write_datasink was called
        mock_dataset.write_datasink.assert_called_once()

        # Get the sink that was passed
        sink = mock_dataset.write_datasink.call_args[0][0]

        assert isinstance(sink, KafkaDatasink)
        assert sink.topic == "test-topic"
        assert sink.bootstrap_servers == "localhost:9092"
        assert sink.key_field == "id"
        assert sink.value_serializer == "string"
        assert sink.batch_size == 200

    def test_write_kafka_minimal_args(self):
        """Test write_kafka with minimal arguments."""
        mock_dataset = Mock()

        write_kafka(
            dataset=mock_dataset,
            topic="test-topic",
            bootstrap_servers="localhost:9092",
        )

        mock_dataset.write_datasink.assert_called_once()

        sink = mock_dataset.write_datasink.call_args[0][0]
        assert sink.topic == "test-topic"
        assert sink.value_serializer == "json"  # default
        assert sink.batch_size == 100  # default

    def test_write_kafka_with_producer_config(self):
        """Test write_kafka passes producer config."""
        mock_dataset = Mock()

        producer_config = {"acks": "all", "retries": 10}

        write_kafka(
            dataset=mock_dataset,
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            producer_config=producer_config,
        )

        sink = mock_dataset.write_datasink.call_args[0][0]
        assert sink.producer_config == producer_config

    def test_write_kafka_with_callback(self):
        """Test write_kafka passes custom callback."""
        mock_dataset = Mock()
        callback = Mock()

        write_kafka(
            dataset=mock_dataset,
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            delivery_callback=callback,
        )

        sink = mock_dataset.write_datasink.call_args[0][0]
        assert sink.delivery_callback == callback


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
