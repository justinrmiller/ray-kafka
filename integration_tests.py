"""
Integration tests that actually write to Kafka using kafka-python.

Run with pytest:
    pytest integration_tests.py -v -m integration

Or with the custom runner:
    python integration_tests.py

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
"""

import json
import os
import time
import uuid
from typing import Any

import pytest
import ray
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from src.kafka_datasink import write_kafka


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    """Get Kafka bootstrap servers from environment."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture(scope="session")
def kafka_admin(kafka_bootstrap_servers):
    """Create a Kafka admin client for the test session."""
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    yield admin
    admin.close()


@pytest.fixture(scope="session")
def ray_context():
    """Initialize Ray for the test session."""
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture
def kafka_topic(kafka_bootstrap_servers):
    """Create a unique Kafka topic for each test and clean up after."""
    topic_name = f"test-{uuid.uuid4().hex[:12]}"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        admin.create_topics([topic])
        # Wait for topic to be ready
        time.sleep(1)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    yield topic_name

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    try:
        admin.delete_topics([topic_name])
    except UnknownTopicOrPartitionError:
        pass
    except Exception:
        pass  # Best effort cleanup
    finally:
        admin.close()


@pytest.fixture
def kafka_topic_multi_partition(kafka_bootstrap_servers):
    """Create a Kafka topic with multiple partitions."""
    topic_name = f"test-mp-{uuid.uuid4().hex[:12]}"

    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=3,
            replication_factor=1,
        )
        admin.create_topics([topic])
        time.sleep(1)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    yield topic_name

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    try:
        admin.delete_topics([topic_name])
    except Exception:
        pass
    finally:
        admin.close()


# =============================================================================
# Helper Functions
# =============================================================================


def consume_messages(
    topic: str,
    bootstrap_servers: str,
    expected_count: int,
    timeout: int = 30,
    value_deserializer: str = "json",
) -> list[dict[str, Any]]:
    """Consume messages from Kafka topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=f"test-consumer-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout * 1000,
    )

    messages = []
    start_time = time.time()

    try:
        for msg in consumer:
            if time.time() - start_time > timeout:
                break

            try:
                if value_deserializer == "json":
                    value = json.loads(msg.value.decode("utf-8"))
                elif value_deserializer == "bytes":
                    value = msg.value  # Keep as bytes
                else:
                    value = msg.value.decode("utf-8")

                messages.append(
                    {
                        "value": value,
                        "key": msg.key.decode("utf-8") if msg.key else None,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                    }
                )
            except Exception as e:
                messages.append({"raw": msg.value, "error": str(e)})

            if len(messages) >= expected_count:
                break

    except StopIteration:
        pass
    finally:
        consumer.close()

    return messages


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.integration
class TestBasicWrite:
    """Tests for basic write functionality."""

    def test_basic_write_json(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test basic write to Kafka with JSON serialization."""
        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer="json",
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, len(data))

        assert len(messages) == len(data)
        received_ids = sorted([msg["value"]["id"] for msg in messages])
        expected_ids = sorted([d["id"] for d in data])
        assert received_ids == expected_ids

    def test_write_empty_dataset(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test writing an empty dataset."""
        ds = ray.data.from_items([])

        # Should complete without error
        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
        )

        time.sleep(1)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, 0, timeout=5)
        assert len(messages) == 0


@pytest.mark.integration
class TestKeyedMessages:
    """Tests for messages with keys."""

    def test_write_with_keys(self, kafka_bootstrap_servers, kafka_topic_multi_partition, ray_context):
        """Test write with message keys for partitioning."""
        data = [
            {"user_id": f"user_{i}", "action": f"action_{i}", "timestamp": i}
            for i in range(10)
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic_multi_partition,
            bootstrap_servers=kafka_bootstrap_servers,
            key_field="user_id",
            value_serializer="json",
        )

        time.sleep(2)
        messages = consume_messages(
            kafka_topic_multi_partition, kafka_bootstrap_servers, len(data)
        )

        assert len(messages) == len(data)

        # Verify all messages have keys
        messages_with_keys = [msg for msg in messages if msg["key"] is not None]
        assert len(messages_with_keys) == len(data)

        # Verify keys match user_ids
        keys = sorted([msg["key"] for msg in messages])
        expected_keys = sorted([d["user_id"] for d in data])
        assert keys == expected_keys

    def test_key_partitioning_consistency(
        self, kafka_bootstrap_servers, kafka_topic_multi_partition, ray_context
    ):
        """Test that same keys always go to same partition."""
        # Write multiple messages with same keys
        data = [
            {"user_id": "alice", "seq": i} for i in range(5)
        ] + [
            {"user_id": "bob", "seq": i} for i in range(5)
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic_multi_partition,
            bootstrap_servers=kafka_bootstrap_servers,
            key_field="user_id",
        )

        time.sleep(2)
        messages = consume_messages(
            kafka_topic_multi_partition, kafka_bootstrap_servers, len(data)
        )

        # Group by key and verify same partition
        key_partitions = {}
        for msg in messages:
            key = msg["key"]
            partition = msg["partition"]
            if key in key_partitions:
                assert key_partitions[key] == partition, f"Key {key} went to multiple partitions"
            else:
                key_partitions[key] = partition


@pytest.mark.integration
class TestSerializers:
    """Tests for different serialization formats."""

    def test_string_serializer(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test string value serialization."""
        data = [
            {"id": 1, "message": "Hello"},
            {"id": 2, "message": "World"},
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer="string",
        )

        time.sleep(2)
        messages = consume_messages(
            kafka_topic, kafka_bootstrap_servers, len(data), value_deserializer="string"
        )

        assert len(messages) == len(data)
        for msg in messages:
            assert isinstance(msg["value"], str)
            assert "id" in msg["value"] and "message" in msg["value"]

    def test_bytes_serializer(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test bytes value serialization."""
        # Create data that will be serialized as bytes
        data = [
            {"data": "binary data 1"},
            {"data": "binary data 2"},
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer="bytes",
        )

        time.sleep(2)
        messages = consume_messages(
            kafka_topic, kafka_bootstrap_servers, len(data), value_deserializer="bytes"
        )

        assert len(messages) == len(data)
        for msg in messages:
            assert isinstance(msg["value"], bytes)


@pytest.mark.integration
class TestUnicodeData:
    """Tests for unicode character handling."""

    def test_unicode_values(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test writing and reading unicode characters in values."""
        data = [
            {"id": 1, "name": "日本語", "emoji": "🎉"},
            {"id": 2, "name": "Ελληνικά", "emoji": "🚀"},
            {"id": 3, "name": "العربية", "emoji": "🌟"},
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer="json",
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, len(data))

        assert len(messages) == len(data)

        # Verify unicode is preserved
        received_names = sorted([msg["value"]["name"] for msg in messages])
        expected_names = sorted([d["name"] for d in data])
        assert received_names == expected_names

        received_emojis = sorted([msg["value"]["emoji"] for msg in messages])
        expected_emojis = sorted([d["emoji"] for d in data])
        assert received_emojis == expected_emojis

    def test_unicode_keys(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test writing unicode characters as keys."""
        data = [
            {"name": "日本語", "value": 1},
            {"name": "Ελληνικά", "value": 2},
        ]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            key_field="name",
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, len(data))

        assert len(messages) == len(data)
        keys = sorted([msg["key"] for msg in messages])
        expected_keys = sorted([d["name"] for d in data])
        assert keys == expected_keys


@pytest.mark.integration
class TestProducerConfiguration:
    """Tests for producer configuration options."""

    def test_custom_producer_config(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test custom producer configuration."""
        data = [{"id": i, "data": f"message_{i}"} for i in range(10)]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config={
                "acks": "all",
                "compression_type": "gzip",
                "retries": 3,
                "linger_ms": 100,
            },
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, len(data))
        assert len(messages) == len(data)

    def test_custom_batch_size(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test different batch sizes."""
        data = [{"id": i} for i in range(50)]
        ds = ray.data.from_items(data)

        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            batch_size=10,
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, len(data))
        assert len(messages) == len(data)


@pytest.mark.integration
class TestLargeDatasets:
    """Tests for larger datasets."""

    def test_large_dataset(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test writing larger dataset with batching."""
        num_records = 1000
        ds = ray.data.range(num_records).map(
            lambda x: {"id": x["id"], "value": x["id"] * 2, "squared": x["id"] ** 2}
        )

        start = time.time()
        write_kafka(
            dataset=ds,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            batch_size=100,
            producer_config={
                "compression_type": "lz4",
                "linger_ms": 10,
            },
        )
        duration = time.time() - start

        # Sample verification
        time.sleep(3)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, 100, timeout=10)
        assert len(messages) > 0

        # Verify structure
        sample = messages[0]["value"]
        assert "id" in sample and "value" in sample and "squared" in sample

    def test_multiple_blocks(self, kafka_bootstrap_servers, kafka_topic, ray_context):
        """Test writing dataset with multiple blocks."""
        num_records = 100
        ds = ray.data.range(num_records).repartition(5)  # Force 5 blocks

        write_kafka(
            dataset=ds.map(lambda x: {"id": x["id"], "value": x["id"] * 2}),
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
        )

        time.sleep(2)
        messages = consume_messages(kafka_topic, kafka_bootstrap_servers, num_records)
        assert len(messages) == num_records


@pytest.mark.integration
class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_bootstrap_servers(self, ray_context):
        """Test error handling with invalid configuration."""
        data = [{"id": 1}]
        ds = ray.data.from_items(data)

        with pytest.raises(ray.exceptions.RayTaskError):
            write_kafka(
                dataset=ds,
                topic="test-error",
                bootstrap_servers="invalid-broker:9092",
                producer_config={
                    "request_timeout_ms": 1000,
                    "max_block_ms": 2000,
                },
            )


# =============================================================================
# Test Runner (for running without pytest)
# =============================================================================


def run_all_tests():
    """Run all integration tests without pytest."""
    print("\n" + "=" * 70)
    print("RAY KAFKA DATASINK - INTEGRATION TESTS")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Bootstrap servers: {bootstrap_servers}")

    # Verify Kafka is accessible
    print("\nVerifying Kafka connection...")
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin.list_topics()
        admin.close()
        print(f"Connected to Kafka (found {len(topics)} topics)")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("\nMake sure Kafka is running:")
        print("  docker-compose up -d kafka")
        return False

    # Initialize Ray
    ray.init(ignore_reinit_error=True)

    tests = [
        ("Basic Write JSON", test_basic_write_json),
        ("Write Empty Dataset", test_write_empty_dataset),
        ("Write With Keys", test_write_with_keys),
        ("String Serializer", test_string_serializer),
        ("Bytes Serializer", test_bytes_serializer),
        ("Unicode Values", test_unicode_values),
        ("Unicode Keys", test_unicode_keys),
        ("Custom Producer Config", test_custom_producer_config),
        ("Custom Batch Size", test_custom_batch_size),
        ("Multiple Blocks", test_multiple_blocks),
        ("Large Dataset", test_large_dataset),
        ("Error Handling", test_error_handling),
    ]

    failed = []
    passed = []

    for name, test_func in tests:
        print(f"\n{'=' * 70}")
        print(f"TEST: {name}")
        print("=" * 70)

        topic_name = f"test-{uuid.uuid4().hex[:12]}"

        # Create topic
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
            admin.create_topics([topic])
            time.sleep(1)
        except TopicAlreadyExistsError:
            pass
        finally:
            admin.close()

        try:
            test_func(bootstrap_servers, topic_name)
            passed.append(name)
            print(f"TEST PASSED: {name}")
        except AssertionError as e:
            failed.append((name, str(e)))
            print(f"TEST FAILED: {name}")
            print(f"   Error: {e}")
        except Exception as e:
            failed.append((name, str(e)))
            print(f"TEST FAILED: {name}")
            print(f"   Unexpected error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Cleanup topic
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            try:
                admin.delete_topics([topic_name])
            except Exception:
                pass
            finally:
                admin.close()

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Total:  {len(tests)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")

    if passed:
        print("\nPassed tests:")
        for name in passed:
            print(f"  {name}")

    if failed:
        print("\nFailed tests:")
        for name, error in failed:
            print(f"  {name}")
            print(f"     {error}")

    print("=" * 70 + "\n")

    ray.shutdown()
    return len(failed) == 0


# Standalone test functions for the custom runner
def test_basic_write_json(bootstrap_servers, topic):
    data = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
        {"id": 3, "name": "Charlie", "score": 92},
    ]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers)
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)


def test_write_empty_dataset(bootstrap_servers, topic):
    ds = ray.data.from_items([])
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers)
    time.sleep(1)
    messages = consume_messages(topic, bootstrap_servers, 0, timeout=5)
    assert len(messages) == 0


def test_write_with_keys(bootstrap_servers, topic):
    data = [{"user_id": f"user_{i}", "action": f"action_{i}"} for i in range(10)]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers, key_field="user_id")
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)
    assert all(msg["key"] is not None for msg in messages)


def test_string_serializer(bootstrap_servers, topic):
    data = [{"id": 1, "message": "Hello"}, {"id": 2, "message": "World"}]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers, value_serializer="string")
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data), value_deserializer="string")
    assert len(messages) == len(data)


def test_bytes_serializer(bootstrap_servers, topic):
    data = [{"data": "binary data 1"}, {"data": "binary data 2"}]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers, value_serializer="bytes")
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data), value_deserializer="bytes")
    assert len(messages) == len(data)


def test_unicode_values(bootstrap_servers, topic):
    data = [{"id": 1, "name": "日本語", "emoji": "🎉"}, {"id": 2, "name": "Ελληνικά", "emoji": "🚀"}]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers)
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)


def test_unicode_keys(bootstrap_servers, topic):
    data = [{"name": "日本語", "value": 1}, {"name": "Ελληνικά", "value": 2}]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers, key_field="name")
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)


def test_custom_producer_config(bootstrap_servers, topic):
    data = [{"id": i} for i in range(10)]
    ds = ray.data.from_items(data)
    write_kafka(
        dataset=ds,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        producer_config={"acks": "all", "compression_type": "gzip"},
    )
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)


def test_custom_batch_size(bootstrap_servers, topic):
    data = [{"id": i} for i in range(50)]
    ds = ray.data.from_items(data)
    write_kafka(dataset=ds, topic=topic, bootstrap_servers=bootstrap_servers, batch_size=10)
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, len(data))
    assert len(messages) == len(data)


def test_multiple_blocks(bootstrap_servers, topic):
    num_records = 100
    ds = ray.data.range(num_records).repartition(5)
    write_kafka(
        dataset=ds.map(lambda x: {"id": x["id"], "value": x["id"] * 2}),
        topic=topic,
        bootstrap_servers=bootstrap_servers,
    )
    time.sleep(2)
    messages = consume_messages(topic, bootstrap_servers, num_records)
    assert len(messages) == num_records


def test_large_dataset(bootstrap_servers, topic):
    num_records = 1000
    ds = ray.data.range(num_records).map(lambda x: {"id": x["id"], "value": x["id"] * 2})
    write_kafka(
        dataset=ds,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        batch_size=100,
        producer_config={"compression_type": "lz4"},
    )
    time.sleep(3)
    messages = consume_messages(topic, bootstrap_servers, 100, timeout=10)
    assert len(messages) > 0


def test_error_handling(bootstrap_servers, topic):
    data = [{"id": 1}]
    ds = ray.data.from_items(data)
    try:
        write_kafka(
            dataset=ds,
            topic="test-error",
            bootstrap_servers="invalid-broker:9092",
            producer_config={"request_timeout_ms": 1000, "max_block_ms": 2000},
        )
        raise AssertionError("Should have raised an error")
    except ray.exceptions.RayTaskError:
        pass  # Expected


if __name__ == "__main__":
    try:
        success = run_all_tests()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        if ray.is_initialized():
            ray.shutdown()
        exit(130)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        if ray.is_initialized():
            ray.shutdown()
        exit(1)
