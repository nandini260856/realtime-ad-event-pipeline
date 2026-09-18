"""Consume ad events from Kafka and maintain per-campaign counters in DynamoDB."""

import json
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer

TABLE_NAME = "campaign_counters"
TOPIC = "ad-events"

def get_table():
    ddb = boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    try:
        ddb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "campaign_id", "KeyType": "HASH"},
                {"AttributeName": "stat_date", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "campaign_id", "AttributeType": "S"},
                {"AttributeName": "stat_date", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        ).wait_until_exists()
        print(f"created table {TABLE_NAME}")
    except ClientError as err:
        if err.response["Error"]["Code"] != "ResourceInUseException":
            raise
    return ddb.Table(TABLE_NAME)

def bump(table, event):
    """Atomically increment counters for one event."""
    is_click = 1 if event["event_type"] == "click" else 0
    table.update_item(
        Key={
            "campaign_id": event["campaign_id"],
            "stat_date": event["event_time"][:10],
        },
        UpdateExpression=(
            "ADD impressions :one, clicks :c, spend_cpm :cpm "
            "SET last_seen = :ts"
        ),
        ExpressionAttributeValues={
            ":one": 1,
            ":c": is_click,
            ":cpm": Decimal(str(event["bid_cpm"])),
            ":ts": event["event_time"],
        },
    )

def main():
    table = get_table()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="campaign-counters",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    processed = 0
    print("consuming... Control+C to stop")
    try:
        for message in consumer:
            bump(table, message.value)
            processed += 1
            if processed % 100 == 0:
                print(f"updated counters for {processed} events")
    except KeyboardInterrupt:
        print(f"\nstopping after {processed} events")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()