"""
Integration tests that actually write to Kafka using confluent-kafka.
Run with: docker-compose --profile integration run integration-tests
Or locally: KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python integration_tests.py
"""

import json
import os
import time
from typing import Any

import ray
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from src.kafka_datasink import write_kafka


def setup_topic(topic_name: str, bootstrap_servers: str, num_partitions: int = 1) -> None:
    """Create topic if it doesn't exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Check if topic exists
    metadata = admin.list_topics(timeout=10)

    if topic_name not in metadata.topics:
        topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=1,
            config={"retention.ms": "3600000"},  # 1 hour retention for tests
        )
        fs = admin.create_topics([topic])

        # Wait for operation to complete
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Created topic: {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
                raise
    else:
        print(f"Topic already exists: {topic_name}")


def delete_topic(topic_name: str, bootstrap_servers: str) -> None:
    """Delete topic for cleanup."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    fs = admin.delete_topics([topic_name], operation_timeout=30)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Deleted topic: {topic}")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")


def consume_messages(
    topic: str,
    bootstrap_servers: str,
    expected_count: int,
    timeout: int = 30,
    value_deserializer: str = "json",
) -> list[dict[str, Any]]:
    """Consume messages from Kafka topic."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-consumer-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    messages = []
    start_time = time.time()

    try:
        while len(messages) < expected_count:
            if time.time() - start_time > timeout:
                print(f"Timeout waiting for messages. Got {len(messages)}/{expected_count}")
                break

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Decode message
            try:
                if value_deserializer == "json":
                    value = json.loads(msg.value().decode("utf-8"))
                else:
                    value = msg.value().decode("utf-8")

                messages.append(
                    {
                        "value": value,
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != -1 else None,
                    }
                )
            except Exception as e:
                print(f"Error decoding message: {e}")
                messages.append({"raw": msg.value(), "error": str(e)})

    finally:
        consumer.close()

    return messages


def test_basic_write():
    """Test basic write to Kafka with JSON serialization."""
    print("\n" + "=" * 70)
    print("TEST: Basic Write")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-basic-write"

    # Setup
    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        # Create dataset
        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]
        ds = ray.data.from_items(data)

        # Write to Kafka
        print(f"Writing {len(data)} records to topic '{topic}'...")
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer="json",
        )
        print("Write completed")

        # Consume and verify
        print("Consuming messages...")
        time.sleep(2)  # Give Kafka time to process
        messages = consume_messages(topic, bootstrap_servers, len(data))

        assert len(messages) == len(data), f"Expected {len(data)} messages, got {len(messages)}"
        print(f"Consumed {len(messages)} messages")

        # Verify content
        received_ids = sorted([msg["value"]["id"] for msg in messages])
        expected_ids = sorted([d["id"] for d in data])
        assert received_ids == expected_ids, f"ID mismatch: {received_ids} != {expected_ids}"
        print("Message content verified")

        print("TEST PASSED: Basic Write\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_write_with_keys():
    """Test write with message keys for partitioning."""
    print("\n" + "=" * 70)
    print("TEST: Write with Keys")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-write-with-keys"

    setup_topic(topic, bootstrap_servers, num_partitions=3)
    ray.init(ignore_reinit_error=True)

    try:
        # Create dataset with user IDs as keys
        data = [
            {"user_id": f"user_{i}", "action": f"action_{i}", "timestamp": i} for i in range(10)
        ]
        ds = ray.data.from_items(data)

        # Write with keys
        print(f"Writing {len(data)} records with keys...")
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            key_field="user_id",
            value_serializer="json",
        )
        print("Write completed")

        # Consume and verify keys
        print("Consuming messages...")
        time.sleep(2)
        messages = consume_messages(topic, bootstrap_servers, len(data))

        assert len(messages) == len(data), f"Expected {len(data)} messages"
        print(f"Consumed {len(messages)} messages")

        # Verify all messages have keys
        messages_with_keys = [msg for msg in messages if msg["key"] is not None]
        assert len(messages_with_keys) == len(data), "Not all messages have keys"
        print("All messages have keys")

        # Verify keys match user_ids
        keys = sorted([msg["key"] for msg in messages])
        expected_keys = sorted([d["user_id"] for d in data])
        assert keys == expected_keys, f"Key mismatch: {keys} != {expected_keys}"
        print("Keys verified")

        # Verify partitioning (messages with same key go to same partition)
        key_partitions = {}
        for msg in messages:
            key = msg["key"]
            partition = msg["partition"]
            if key in key_partitions:
                assert key_partitions[key] == partition, f"Key {key} went to multiple partitions"
            else:
                key_partitions[key] = partition
        print(f"Partitioning verified ({len(set(key_partitions.values()))} partitions used)")

        print("TEST PASSED: Write with Keys\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_large_dataset():
    """Test writing larger dataset with batching."""
    print("\n" + "=" * 70)
    print("TEST: Large Dataset")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-large-dataset"

    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        # Create larger dataset
        num_records = 1000
        ds = ray.data.range(num_records).map(
            lambda x: {"id": x["id"], "value": x["id"] * 2, "squared": x["id"] ** 2}
        )

        print(f"Writing {num_records} records...")
        start = time.time()
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            batch_size=100,
            producer_config={
                "compression.type": "lz4",
                "linger.ms": 10,
            },
        )
        duration = time.time() - start
        print(f"Write completed in {duration:.2f}s ({num_records / duration:.0f} msgs/sec)")

        # Sample verification (consume first 100 messages)
        print("Verifying sample of messages...")
        time.sleep(3)
        messages = consume_messages(topic, bootstrap_servers, 100, timeout=10)
        assert len(messages) > 0, "Should have consumed at least some messages"

        # Verify structure
        sample = messages[0]["value"]
        assert "id" in sample and "value" in sample and "squared" in sample
        print(f"Verified {len(messages)} messages")

        print("TEST PASSED: Large Dataset\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_string_serializer():
    """Test string value serialization."""
    print("\n" + "=" * 70)
    print("TEST: String Serializer")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-string-serializer"

    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        # Create dataset with dict data (will be stringified)
        data = [
            {"id": 1, "message": "Hello"},
            {"id": 2, "message": "World"},
        ]
        ds = ray.data.from_items(data)

        print("Writing with string serializer...")
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer="string",
        )
        print("Write completed")

        # Consume as strings
        time.sleep(2)
        messages = consume_messages(
            topic, bootstrap_servers, len(data), value_deserializer="string"
        )

        assert len(messages) == len(data)
        print(f"Consumed {len(messages)} string messages")

        # Verify all messages are strings containing expected content
        for msg in messages:
            assert isinstance(msg["value"], str)
            assert "id" in msg["value"] and "message" in msg["value"]
        print("String serialization verified")

        print("TEST PASSED: String Serializer\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_producer_config():
    """Test custom producer configuration."""
    print("\n" + "=" * 70)
    print("TEST: Producer Config")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-producer-config"

    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        data = [{"id": i, "data": f"message_{i}"} for i in range(10)]
        ds = ray.data.from_items(data)

        print("Writing with custom producer config...")
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            producer_config={
                "acks": "all",
                "compression.type": "gzip",
                "retries": 3,
                "linger.ms": 100,
            },
        )
        print("Write completed with custom config")

        # Verify messages arrived
        time.sleep(2)
        messages = consume_messages(topic, bootstrap_servers, len(data))
        assert len(messages) == len(data)
        print(f"All {len(messages)} messages received")

        print("TEST PASSED: Producer Config\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_custom_batch_size():
    """Test different batch sizes."""
    print("\n" + "=" * 70)
    print("TEST: Custom Batch Size")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-batch-size"

    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        # Test with small batch size
        data = [{"id": i} for i in range(50)]
        ds = ray.data.from_items(data)

        print("Writing with batch_size=10...")
        start = time.time()
        write_kafka(
            dataset=ds,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            batch_size=10,
        )
        duration = time.time() - start
        print(f"Write completed in {duration:.2f}s")

        # Verify
        time.sleep(2)
        messages = consume_messages(topic, bootstrap_servers, len(data))
        assert len(messages) == len(data)
        print(f"All {len(messages)} messages received")

        print("TEST PASSED: Custom Batch Size\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_multiple_blocks():
    """Test writing dataset with multiple blocks."""
    print("\n" + "=" * 70)
    print("TEST: Multiple Blocks")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "test-multiple-blocks"

    setup_topic(topic, bootstrap_servers)
    ray.init(ignore_reinit_error=True)

    try:
        # Create dataset with multiple blocks
        num_records = 100
        ds = ray.data.range(num_records).repartition(5)  # Force 5 blocks

        print(f"Writing {num_records} records across multiple blocks...")
        write_kafka(
            dataset=ds.map(lambda x: {"id": x["id"], "value": x["id"] * 2}),
            topic=topic,
            bootstrap_servers=bootstrap_servers,
        )
        print("Write completed")

        # Verify all records
        time.sleep(2)
        messages = consume_messages(topic, bootstrap_servers, num_records)
        assert len(messages) == num_records
        print(f"All {len(messages)} messages from multiple blocks received")

        print("TEST PASSED: Multiple Blocks\n")

    finally:
        delete_topic(topic, bootstrap_servers)


def test_error_handling():
    """Test error handling with invalid configuration."""
    print("\n" + "=" * 70)
    print("TEST: Error Handling")
    print("=" * 70)

    ray.init(ignore_reinit_error=True)

    try:
        data = [{"id": 1}]
        ds = ray.data.from_items(data)

        # Test with invalid broker (should fail)
        print("Testing with invalid bootstrap servers...")
        try:
            write_kafka(
                dataset=ds,
                topic="test-error",
                bootstrap_servers="invalid-broker:9092",
                producer_config={
                    "socket.timeout.ms": 1000,
                    "message.timeout.ms": 2000,
                },
            )
            raise AssertionError("Should have raised an error")
        except Exception as e:
            print(f"Caught expected error: {type(e).__name__}")

        print("TEST PASSED: Error Handling\n")

    except Exception as e:
        print(f"Test failed with unexpected error: {e}")
        raise


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "=" * 70)
    print("RAY KAFKA DATASINK - INTEGRATION TESTS")
    print("=" * 70)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Bootstrap servers: {bootstrap_servers}")

    # Verify Kafka is accessible
    print("\nVerifying Kafka connection...")
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        print(f"Connected to Kafka (found {len(metadata.topics)} topics)")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("\nMake sure Kafka is running:")
        print("  docker-compose up -d kafka")
        return False

    tests = [
        test_basic_write,
        test_write_with_keys,
        test_string_serializer,
        test_producer_config,
        test_custom_batch_size,
        test_multiple_blocks,
        test_large_dataset,
        test_error_handling,
    ]

    failed = []
    passed = []

    for test in tests:
        try:
            test()
            passed.append(test.__name__)
        except AssertionError as e:
            failed.append((test.__name__, str(e)))
            print(f"TEST FAILED: {test.__name__}")
            print(f"   Error: {e}\n")
        except Exception as e:
            failed.append((test.__name__, str(e)))
            print(f"TEST FAILED: {test.__name__}")
            print(f"   Unexpected error: {e}\n")
            import traceback

            traceback.print_exc()

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Total:  {len(tests)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")

    if passed:
        print("\nPassed tests:")
        for test_name in passed:
            print(f"  {test_name}")

    if failed:
        print("\nFailed tests:")
        for test_name, error in failed:
            print(f"  {test_name}")
            print(f"     {error}")

    print("=" * 70 + "\n")

    return len(failed) == 0


if __name__ == "__main__":
    try:
        success = run_all_tests()

        # Cleanup Ray
        if ray.is_initialized():
            ray.shutdown()

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
