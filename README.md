# realtime-ad-event-pipeline
Streaming ad event pipeline: Kafka → Iceberg → DynamoDB
# Real-Time Ad Event Pipeline

Streaming pipeline for ad impression events: Kafka → Spark Structured Streaming → Apache Iceberg,
with a DynamoDB serving layer for low-latency per-campaign counters.

## Architecture

    producer.py ──> Kafka (ad-events, 3 partitions, keyed by campaign_id)
                      │
                      ├──> stream_to_iceberg.py  ──> Iceberg table (partitioned by date, campaign)
                      │       Spark Structured Streaming, 30s micro-batches,
                      │       checkpointed offsets, dedup on event_id
                      │
                      └──> counters.py ──> DynamoDB (campaign_id + stat_date, atomic ADD)

    analytics.py ──> z-score anomaly detection + A/B split reporting over the Iceberg table

## Design notes

- **Partition key** — events are keyed by `campaign_id` so each campaign's events stay ordered
  within a partition.
- **Exactly-once-ish** — Kafka gives at-least-once delivery; dedup on `event_id` plus Spark
  checkpointing makes reprocessing safe.
- **Iceberg partitioning** — `(event_date, campaign_id)` so filtered queries prune files.
- **DynamoDB `ADD`** — server-side atomic increments avoid lost updates from concurrent consumers.
- **A/B assignment** — `crc32(user_hash) % 2` is deterministic, so a user never crosses variants.
- **Anomaly detection** — per-campaign z-score with a minimum sample size. Limitation: assumes no
  seasonality; production traffic would need a rolling or seasonal baseline.

## Running it

    docker compose up -d
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic ad-events \
      --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

    python3 src/stream_to_iceberg.py        # terminal 1
    cd src && python3 producer.py --count 1000 --rate 30   # terminal 2
    python3 src/counters.py                 # terminal 3
    python3 src/analytics.py                # after a batch commits

    docker compose down