# Real-Time Shipment Tracking

A real-time data engineering project using **Apache Kafka + Scala + Apache Spark Structured Streaming**.

## Architecture

Shipment Producer -> Kafka (`shipment-events`) -> Spark Structured Streaming -> Real-Time Aggregation -> Console

## Technologies

- Scala 2.13
- Apache Kafka 4.0.1
- Apache Spark 4.0.1
- SBT
- Docker
- Spark Structured Streaming

## How to run

### 1. Start Kafka

```powershell
docker compose up -d
```

### 2. Create the topic

```powershell
docker exec shipment-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic shipment-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Start Spark Streaming

Open Terminal 1:

```powershell
sbt "runMain com.shipment.ShipmentStreaming"
```

### 4. Start the live shipment producer

Open Terminal 2:

```powershell
sbt "runMain com.shipment.ShipmentProducer"
```

The producer generates shipment events every 2 seconds. Spark consumes them from Kafka and maintains a 30-second real-time status aggregation.

## Sample event

```json
{
  "shipment_id": "SHP-1234",
  "location": "Hyderabad",
  "status": "IN_TRANSIT",
  "latitude": 17.385,
  "longitude": 78.486,
  "event_time": 1770000000000
}
```

## Interview explanation

The producer continuously generates shipment GPS/status events and publishes them to Kafka. Kafka provides the durable event-streaming layer. Spark Structured Streaming consumes the Kafka topic, parses the JSON events, applies event-time processing and watermarking, and performs real-time aggregations by shipment status. Checkpointing allows Spark to maintain streaming progress and recover from failures.
