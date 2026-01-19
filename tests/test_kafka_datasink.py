"""Unit tests for Kafka datasink with mocked dependencies."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
from kafka.errors import KafkaError, KafkaTimeoutError

from src.kafka_datasink import KafkaDatasink, write_kafka


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_producer():
    """Create a mock KafkaProducer with successful futures."""
    with patch("src.kafka_datasink.KafkaProducer") as mock_class:
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock()
        mock_instance.send.return_value = mock_future
        mock_class.return_value = mock_instance
        yield mock_instance, mock_class, mock_future


@pytest.fixture
def mock_producer_with_failed_futures():
    """Create a mock KafkaProducer with failing futures."""
    with patch("src.kafka_datasink.KafkaProducer") as mock_class:
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = Exception("Delivery failed")
        mock_instance.send.return_value = mock_future
        mock_class.return_value = mock_instance
        yield mock_instance, mock_class, mock_future


@pytest.fixture
def mock_block_accessor():
    """Create a mock BlockAccessor."""
    with patch("src.kafka_datasink.BlockAccessor") as mock_class:
        mock_instance = MagicMock()
        mock_class.for_block.return_value = mock_instance
        yield mock_instance, mock_class


@pytest.fixture
def sample_rows():
    """Sample row data for testing."""
    return [
        {"id": 1, "value": "foo"},
        {"id": 2, "value": "bar"},
    ]


@pytest.fixture
def sample_unicode_rows():
    """Sample row data with unicode characters."""
    return [
        {"id": 1, "name": "日本語", "emoji": "🎉"},
        {"id": 2, "name": "Ελληνικά", "emoji": "🚀"},
    ]


# =============================================================================
# Initialization Tests
# =============================================================================


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
        assert sink.producer_config == {}

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

    def test_init_producer_config_defaults_to_empty_dict(self):
        """Test that producer_config defaults to empty dict, not None."""
        sink = KafkaDatasink("test", "localhost:9092", producer_config=None)
        assert sink.producer_config == {}


# =============================================================================
# Row Conversion Tests
# =============================================================================


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
        assert sink._row_to_dict(3.14) == 3.14
        assert sink._row_to_dict(None) is None

    def test_row_to_dict_with_list(self):
        """Test that lists pass through unchanged."""
        sink = KafkaDatasink("test", "localhost:9092")

        test_list = [1, 2, 3]
        assert sink._row_to_dict(test_list) == test_list


# =============================================================================
# Serializer Tests
# =============================================================================


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

        mock_arrow_row = Mock()
        mock_arrow_row.as_pydict.return_value = {"id": 1, "value": "foo"}

        serialized = sink._serialize_value(mock_arrow_row)

        assert isinstance(serialized, bytes)
        deserialized = json.loads(serialized.decode("utf-8"))
        assert deserialized == {"id": 1, "value": "foo"}

    def test_json_serializer_with_unicode(self, sample_unicode_rows):
        """Test JSON serializer handles unicode characters."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="json",
        )

        for row in sample_unicode_rows:
            serialized = sink._serialize_value(row)
            deserialized = json.loads(serialized.decode("utf-8"))
            assert deserialized == row

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

    def test_string_serializer_with_unicode(self):
        """Test string serializer handles unicode."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="string",
        )

        test_str = "Hello 日本語 🎉"
        serialized = sink._serialize_value(test_str)

        assert isinstance(serialized, bytes)
        assert serialized.decode("utf-8") == test_str

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

    def test_bytes_serializer_with_binary_data(self):
        """Test bytes serializer with binary data."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            value_serializer="bytes",
        )

        # Binary data that's not valid UTF-8
        binary_data = bytes([0x00, 0xFF, 0x80, 0x7F])
        result = sink._serialize_value(binary_data)

        assert result == binary_data


# =============================================================================
# Key Extraction Tests
# =============================================================================


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

    def test_extract_key_with_string_value(self):
        """Test key extraction with string key value."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="user_id",
        )

        row = {"user_id": "alice", "action": "login"}
        key = sink._extract_key(row)

        assert key == b"alice"

    def test_extract_key_with_arrow_row(self):
        """Test key extraction from ArrowRow."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="user_id",
        )

        mock_arrow_row = Mock()
        mock_arrow_row.as_pydict.return_value = {"user_id": "alice", "action": "login"}

        key = sink._extract_key(mock_arrow_row)

        assert key == b"alice"

    def test_extract_key_with_unicode(self):
        """Test key extraction with unicode key value."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field="name",
        )

        row = {"name": "日本語"}
        key = sink._extract_key(row)

        assert key == "日本語".encode("utf-8")

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

    def test_extract_key_no_key_field_configured(self):
        """Test key extraction when no key_field is configured."""
        sink = KafkaDatasink(
            topic="test",
            bootstrap_servers="localhost:9092",
            key_field=None,
        )

        row = {"id": 123, "value": "foo"}
        key = sink._extract_key(row)

        assert key is None


# =============================================================================
# Write Method Tests
# =============================================================================


class TestWriteMethod:
    """Tests for the write method with mocked producer."""

    def test_write_basic(self, mock_producer, mock_block_accessor, sample_rows):
        """Test basic write functionality."""
        mock_prod, mock_prod_class, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = sample_rows

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
        )

        result = sink.write([Mock()], Mock())

        assert mock_prod.send.call_count == 2
        assert mock_prod.flush.called
        assert mock_prod.close.called
        assert result["total_records"] == 2
        assert result["failed_messages"] == 0

    def test_write_sends_to_correct_topic(self, mock_producer, mock_block_accessor):
        """Test that messages are sent to the correct topic."""
        mock_prod, _, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = [{"id": 1}]

        sink = KafkaDatasink("my-specific-topic", "localhost:9092")
        sink.write([Mock()], Mock())

        call_args = mock_prod.send.call_args
        assert call_args[0][0] == "my-specific-topic"

    def test_write_with_keys(self, mock_producer, mock_block_accessor, sample_rows):
        """Test write with key extraction."""
        mock_prod, _, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = sample_rows

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_field="id",
        )

        sink.write([Mock()], Mock())

        for call_args in mock_prod.send.call_args_list:
            assert call_args[1]["key"] is not None
            assert isinstance(call_args[1]["key"], bytes)

    def test_write_without_key_field(self, mock_producer, mock_block_accessor):
        """Test that key is None when key_field is not specified."""
        mock_prod, _, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = [{"id": 1}]

        sink = KafkaDatasink("test-topic", "localhost:9092")  # No key_field
        sink.write([Mock()], Mock())

        call_kwargs = mock_prod.send.call_args[1]
        assert call_kwargs["key"] is None

    def test_write_counts_failed_messages(self, mock_producer_with_failed_futures, mock_block_accessor):
        """Test that failed futures are counted."""
        mock_prod, _, _ = mock_producer_with_failed_futures
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = [{"id": 1}, {"id": 2}]

        sink = KafkaDatasink("test-topic", "localhost:9092")
        result = sink.write([Mock()], Mock())

        assert result["total_records"] == 2
        assert result["failed_messages"] == 2

    def test_write_empty_blocks(self, mock_producer, mock_block_accessor):
        """Test writing with empty blocks."""
        mock_prod, _, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = []

        sink = KafkaDatasink("test-topic", "localhost:9092")
        result = sink.write([Mock()], Mock())

        assert result["total_records"] == 0
        assert result["failed_messages"] == 0
        assert mock_prod.send.call_count == 0
        assert mock_prod.flush.called  # Final flush still called
        assert mock_prod.close.called

    def test_write_multiple_blocks(self, mock_producer, mock_block_accessor):
        """Test writing multiple blocks."""
        mock_prod, _, _ = mock_producer
        mock_accessor, mock_accessor_class = mock_block_accessor

        # Create separate accessor instances for each block
        accessor1 = MagicMock()
        accessor1.iter_rows.return_value = [{"id": 1}]
        accessor2 = MagicMock()
        accessor2.iter_rows.return_value = [{"id": 2}, {"id": 3}]

        mock_accessor_class.for_block.side_effect = [accessor1, accessor2]

        sink = KafkaDatasink("test-topic", "localhost:9092")
        result = sink.write([Mock(), Mock()], Mock())

        assert result["total_records"] == 3
        assert mock_prod.send.call_count == 3

    def test_write_buffer_error_retry(self, mock_block_accessor):
        """Test BufferError triggers retry."""
        with patch("src.kafka_datasink.KafkaProducer") as mock_prod_class:
            mock_prod = MagicMock()
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock()
            mock_prod.send.side_effect = [
                BufferError("Queue full"),
                mock_future,
            ]
            mock_prod_class.return_value = mock_prod

            mock_accessor, _ = mock_block_accessor
            mock_accessor.iter_rows.return_value = [{"id": 1}]

            sink = KafkaDatasink("test-topic", "localhost:9092")
            sink.write([Mock()], Mock())

            assert mock_prod.flush.called
            assert mock_prod.send.call_count == 2

    def test_write_kafka_timeout_error(self, mock_block_accessor):
        """Test KafkaTimeoutError handling."""
        with patch("src.kafka_datasink.KafkaProducer") as mock_prod_class:
            mock_prod = MagicMock()
            mock_prod.send.side_effect = KafkaTimeoutError("Timeout")
            mock_prod_class.return_value = mock_prod

            mock_accessor, _ = mock_block_accessor
            mock_accessor.iter_rows.return_value = [{"id": 1}]

            sink = KafkaDatasink("test-topic", "localhost:9092")

            with pytest.raises(RuntimeError, match="Failed to write to Kafka"):
                sink.write([Mock()], Mock())

            assert mock_prod.close.called

    def test_write_kafka_error(self, mock_block_accessor):
        """Test KafkaError handling."""
        with patch("src.kafka_datasink.KafkaProducer") as mock_prod_class:
            mock_prod = MagicMock()
            mock_prod.send.side_effect = KafkaError("Kafka error")
            mock_prod_class.return_value = mock_prod

            mock_accessor, _ = mock_block_accessor
            mock_accessor.iter_rows.return_value = [{"id": 1}]

            sink = KafkaDatasink("test-topic", "localhost:9092")

            with pytest.raises(RuntimeError, match="Failed to write to Kafka"):
                sink.write([Mock()], Mock())

            assert mock_prod.close.called

    def test_write_batching(self, mock_producer, mock_block_accessor):
        """Test that batching triggers flushing."""
        mock_prod, _, _ = mock_producer

        rows = [{"id": i} for i in range(150)]
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = rows

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            batch_size=100,
        )

        result = sink.write([Mock()], Mock())

        # Should have flushed at 100 records plus final flush
        assert mock_prod.flush.call_count >= 2
        assert result["total_records"] == 150

    def test_write_custom_callback(self, mock_producer, mock_block_accessor):
        """Test custom delivery callback is used."""
        _, _, mock_future = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = [{"id": 1}]

        callback = Mock()
        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            delivery_callback=callback,
        )

        sink.write([Mock()], Mock())

        assert mock_future.add_callback.called
        assert mock_future.add_errback.called

    def test_write_producer_config(self, mock_block_accessor):
        """Test producer config is passed correctly."""
        with patch("src.kafka_datasink.KafkaProducer") as mock_prod_class:
            mock_prod = MagicMock()
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock()
            mock_prod.send.return_value = mock_future
            mock_prod_class.return_value = mock_prod

            mock_accessor, _ = mock_block_accessor
            mock_accessor.iter_rows.return_value = [{"id": 1}]

            custom_config = {"acks": "all", "retries": 5, "compression_type": "gzip"}
            sink = KafkaDatasink(
                topic="test-topic",
                bootstrap_servers="localhost:9092",
                producer_config=custom_config,
            )

            sink.write([Mock()], Mock())

            call_kwargs = mock_prod_class.call_args[1]
            assert call_kwargs["bootstrap_servers"] == "localhost:9092"
            assert call_kwargs["acks"] == "all"
            assert call_kwargs["retries"] == 5
            assert call_kwargs["compression_type"] == "gzip"

    def test_write_with_unicode_data(self, mock_producer, mock_block_accessor, sample_unicode_rows):
        """Test writing unicode data."""
        mock_prod, _, _ = mock_producer
        mock_accessor, _ = mock_block_accessor
        mock_accessor.iter_rows.return_value = sample_unicode_rows

        sink = KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_field="name",
        )

        result = sink.write([Mock()], Mock())

        assert result["total_records"] == 2
        # Verify unicode keys were encoded
        for call_args in mock_prod.send.call_args_list:
            key = call_args[1]["key"]
            assert isinstance(key, bytes)


# =============================================================================
# Delivery Callback Tests
# =============================================================================


class TestDeliveryCallback:
    """Tests for delivery callback."""

    def test_default_delivery_callback_success(self):
        """Test default callback with successful delivery."""
        sink = KafkaDatasink("test", "localhost:9092")

        # Should not raise
        sink._default_delivery_callback(metadata=Mock())

    def test_default_delivery_callback_error(self):
        """Test default callback with delivery error."""
        sink = KafkaDatasink("test", "localhost:9092")

        error = Exception("Delivery failed")

        with pytest.raises(KafkaError, match="Message delivery failed"):
            sink._default_delivery_callback(exception=error)

    def test_default_delivery_callback_no_args(self):
        """Test default callback with no arguments."""
        sink = KafkaDatasink("test", "localhost:9092")

        # Should not raise when both are None/default
        sink._default_delivery_callback()


# =============================================================================
# write_kafka Helper Function Tests
# =============================================================================


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

        mock_dataset.write_datasink.assert_called_once()

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
        assert sink.key_field is None  # default

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

    def test_write_kafka_returns_dataset_result(self):
        """Test write_kafka returns the result from write_datasink."""
        mock_dataset = Mock()
        expected_result = {"total_records": 100, "failed_messages": 0}
        mock_dataset.write_datasink.return_value = expected_result

        result = write_kafka(
            dataset=mock_dataset,
            topic="test-topic",
            bootstrap_servers="localhost:9092",
        )

        assert result == expected_result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
