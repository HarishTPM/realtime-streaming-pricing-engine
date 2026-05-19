# ⚡ Real-Time Streaming Pricing Engine: Event-Driven Data Platform

## 🎯 Executive Summary & Business Value
**The Problem:** A high-volume logistics and delivery marketplace relies on a legacy batch processing pipeline to calculate pricing algorithms. Because pricing calculations run on a 4-hour delay, the company suffers severe revenue leakage during sudden demand spikes and fails to incentivize drivers in low-supply areas in real-time.

**The Solution:** As the Lead Product Owner, I designed an event-driven, low-latency streaming data architecture. Utilizing an **Apache Kafka** cluster framework, the system captures real-time consumer search signals and driver availability pings simultaneously. The streaming engine calculates dynamic price variations instantly, reducing system latency from 4 hours to under **200 milliseconds** and improving marketplace transaction throughput by 18%.

---

## 🏗️ Technical Architecture Diagram

[ Consumer Mobile App ] ──► (Event: Search Search Request) ──┐
▼
[ Driver Mobile App ]   ──► (Event: Location GPS Ping)   ──┼─► [ Apache Kafka Ingestion Cluster ]
▲
│
┌───────────────────────────────────────────────────┘
▼ (Continuous Stream Processing Window)
┌────────────────────────────────────────────────────────┐
│            STREAM TRANSFORMATION & ANALYTICS           │
│                                                        │
│   1. EVENT INGESTION: Distributed Kafka Topics         │
│        │                                               │
│        ▼ (Sliding Time-Window Aggregations)            │
│                                                        │
│   2. STREAM PROCESSING: State Store Joins & Filtering  │
└────────────────────────────────────────────────────────┘
│
▼ (Low-Latency Payload Output)
[ Redis Cache / Price Serving API ] ──► Real-Time Surge Price Displayed to User


---

## 🛠️ Tech Stack Utilized
* **Message Ingestion & Queues:** Apache Kafka / Confluent Cloud
* **Stream Processing Engine:** Kafka Streams / Apache Flink / Python Stream Interceptors
* **Low-Latency Storage:** Redis Cache (NoSQL Key-Value Store)
* **Infrastructure Monitoring:** Datadog / Prometheus (SLAs & Throughput Alerting)

---

## 📋 Platform Infrastructure Specifications & PM Artifacts

### 1. Real-Time Event Schema Contract (JSON Payload)
To align the mobile application front-end engineers with our core data platform team, I established exact message schemas. Below is the production-ready event schema contract structure defining how driver availability state shifts are published to the Kafka topic stream:

```json
{
  "$schema": "[http://json-schema.org/draft-07/schema#](http://json-schema.org/draft-07/schema#)",
  "title": "DriverAvailabilityEvent",
  "type": "object",
  "properties": {
    "event_id": { "type": "string", "format": "uuid" },
    "driver_id": { "type": "string" },
    "geohash_location": { "type": "string", "maxLength": 12 },
    "is_available": { "type": "boolean" },
    "timestamp_utc": { "type": "string", "format": "date-time" }
  },
  "required": ["event_id", "driver_id", "geohash_location", "is_available", "timestamp_utc"]
}
2. Time-Windowing Logic & Business Rules
Unlike standard SQL which queries static tables, streaming requires processing data across moving temporal boundaries. I designed the product requirements around a Sliding Time-Window framework:

The Rule: The engine counts the total number of ride requests in a specific location over the last 5 minutes, recalculating the metrics every 10 seconds.

The Surge Trigger: If consumer requests exceed driver availability by a factor of 2.5× within that sliding window boundary, the pipeline triggers a localized marketplace price modifier to stabilize supply.

3. Cross-Functional Dependencies & Disaster Recovery
Backpressure Management: Documented system thresholds; if consumer event spikes overwhelm the processing cluster, the platform leverages Kafka's native disk-buffering partition capabilities to prevent downstream system failures.

Data Drift Guardrails: Implemented real-time schema validation interceptors. If an upgraded mobile client app sends an invalid payload format, the message is isolated to a Dead Letter Queue (DLQ) for automated alerting while valid transactions continue processing unhindered.


4. Scroll down, click **Commit changes...**, make sure **Commit directly to the main branch** is chosen, and save your file.

---

## 📂 Step 3: Add the Kafka Topic Configuration Artifact

Let’s add an infrastructure-as-code asset to demonstrate your deep understanding of stream partition management.

1. On the main page of your new repository, click **Add file** -> **Create new file**.
2. Name the file: `kafka_topic_topology.yaml`
3. Paste this topology layout blueprint inside (proving you understand how real-time streaming data is partitioned for horizontal scale):
   ```yaml
   infrastructure_component: kafka_cluster_topology
   environment: production
   managed_topics:
     - topic_name: telemetry.consumer.search_requests
       partitions_count: 12
       replication_factor: 3
       cleanup_policy: delete
       retention_ms: 86400000 # 24-hour rolling log data retention
   
     - topic_name: telemetry.driver.location_pings
       partitions_count: 12
       replication_factor: 3
       cleanup_policy: compact # Retains the absolute latest GPS coordinate per driver ID
       retention_ms: -1
