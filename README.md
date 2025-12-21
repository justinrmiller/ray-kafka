# ray-kafka

A Ray Data sink for writing to Apache Kafka using confluent-kafka.

This library provides a `KafkaDatasink` that integrates Ray's distributed data processing with Kafka's streaming platform, leveraging the high-performance librdkafka C library for efficient writes.

## Features

- Write Ray Datasets directly to Kafka topics
- Configurable serialization (JSON, string, bytes)
- Message key extraction for partitioning and compaction
- Asynchronous delivery with customizable callbacks
- Buffer overflow handling with automatic backpressure
- Configurable batching and polling strategies
- Full librdkafka configuration support

## Requirements

- Python 3.11+
- Apache Kafka broker

## Installation

Install from source with uv:

```bash
git clone https://github.com/justinrmiller/ray-kafka.git
cd ray-kafka
uv pip install -e .
```

## Quick Start

```python
import ray
from src.kafka_datasink import write_kafka

# Create a Ray Dataset
ds = ray.data.from_items([
    {"user_id": "alice", "action": "login", "timestamp": 1234567890},
    {"user_id": "bob", "action": "purchase", "timestamp": 1234567891},
])

# Write to Kafka
write_kafka(
    dataset=ds,
    topic="user-events",
    bootstrap_servers="localhost:9092",
    key_field="user_id",
)
```

## API Reference

### KafkaDatasink

```python
from src.kafka_datasink import KafkaDatasink

sink = KafkaDatasink(
    topic="my-topic",
    bootstrap_servers="localhost:9092",
    key_field="id",                    # Optional: field to use as message key
    value_serializer="json",           # "json", "string", or "bytes"
    producer_config={},                # Additional librdkafka configuration
    batch_size=100,                    # Records to batch before polling
    delivery_callback=None,            # Custom delivery report callback
)

dataset.write_datasink(sink)
```

### write_kafka

Convenience function that wraps `KafkaDatasink`:

```python
from src.kafka_datasink import write_kafka

result = write_kafka(
    dataset=ds,
    topic="my-topic",
    bootstrap_servers="localhost:9092",
    key_field=None,
    value_serializer="json",
    producer_config=None,
    batch_size=100,
    delivery_callback=None,
)
# Returns: {"total_records": N, "failed_messages": M}
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `topic` | str | required | Kafka topic name |
| `bootstrap_servers` | str | required | Comma-separated broker addresses |
| `key_field` | str \| None | None | Field name to use as message key |
| `value_serializer` | str | "json" | Serialization format: "json", "string", or "bytes" |
| `producer_config` | dict \| None | None | Additional librdkafka configuration |
| `batch_size` | int | 100 | Number of records before polling for delivery reports |
| `delivery_callback` | Callable \| None | None | Custom callback for delivery reports |

## Configuration Examples

### High-Throughput

```python
write_kafka(
    dataset=ds,
    topic="high-volume",
    bootstrap_servers="broker1:9092,broker2:9092",
    batch_size=500,
    producer_config={
        "acks": "1",
        "compression.type": "lz4",
        "linger.ms": 100,
        "batch.size": 1000000,
        "queue.buffering.max.messages": 100000,
    },
)
```

### High-Reliability

```python
write_kafka(
    dataset=ds,
    topic="critical-events",
    bootstrap_servers="broker1:9092,broker2:9092",
    producer_config={
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 10,
        "retry.backoff.ms": 100,
    },
)
```

### With TLS/SSL

```python
write_kafka(
    dataset=ds,
    topic="secure-topic",
    bootstrap_servers="broker:9093",
    producer_config={
        "security.protocol": "SSL",
        "ssl.ca.location": "/path/to/ca-cert.pem",
        "ssl.certificate.location": "/path/to/client-cert.pem",
        "ssl.key.location": "/path/to/client-key.pem",
    },
)
```

### With SASL Authentication

```python
write_kafka(
    dataset=ds,
    topic="authenticated-topic",
    bootstrap_servers="broker:9092",
    producer_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "your-username",
        "sasl.password": "your-password",
    },
)
```

### Custom Delivery Callback

```python
def track_delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

write_kafka(
    dataset=ds,
    topic="tracked-events",
    bootstrap_servers="localhost:9092",
    delivery_callback=track_delivery,
)
```

## Development

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- uv (recommended) or pip

### Setup

```bash
# Clone the repository
git clone https://github.com/justinrmiller/ray-kafka.git
cd ray-kafka

# Install dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

### Running Tests

#### Unit Tests (Mocked, No Kafka Required)

```bash
# Using Docker
make unit
```

#### Integration Tests (Requires Kafka)

```bash
# Using Docker (starts Kafka automatically)
make integration

### Docker Services

```bash
# Start Kafka only
make up

# Stop all services
make down

# Clean up (remove volumes)
make clean

# Start with Schema Registry
make schema

# Start with Kafka Connect
make connect
```

### Code Quality

```bash
# Format code
make format

# Lint code
make lint

# Check formatting and linting
make check
```

## Project Structure

```
ray-kafka/
├── src/
│   ├── __init__.py
│   └── kafka_datasink.py      # KafkaDatasink and write_kafka
├── tests/
│   └── test_kafka_datasink.py # Unit tests (mocked)
├── integration_tests.py        # Integration tests
├── docker-compose.yaml         # Kafka and test containers
├── Dockerfile.test             # Test container
├── Makefile                    # Build and test commands
└── pyproject.toml              # Project configuration
```

## How It Works

1. **Per-Task Producers**: Each Ray task creates its own Kafka producer to avoid connection pool contention.

2. **Asynchronous Writes**: Messages are produced asynchronously using `producer.produce()` with delivery callbacks.

3. **Buffer Management**: When the producer queue fills, the sink polls for delivery reports and retries.

4. **Batched Polling**: Delivery reports are polled every N records (configurable via `batch_size`) to balance throughput and responsiveness.

5. **Final Flush**: A blocking flush ensures all messages are delivered before the task completes.
