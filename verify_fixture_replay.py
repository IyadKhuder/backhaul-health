import json

from kafka import KafkaConsumer, TopicPartition


partition = TopicPartition("backhaul-link-kpis", 0)
start_offset, end_offset = 0, 8

consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id=None,
    enable_auto_commit=False,
    consumer_timeout_ms=10_000,
    auto_offset_reset="none",
)

next_offset = start_offset
measurement_count = 0

try:
    consumer.assign([partition])
    consumer.seek(partition, start_offset)

    for record in consumer:
        if record.offset != next_offset:
            raise ValueError(f"Expected offset {next_offset}, received {record.offset}")

        key = record.key.decode("utf-8")
        event = json.loads(record.value)
        day = event["observation_date"]

        if key != event["mw_connection_no"] or event["event_id"] != f"{key}|{day}":
            raise ValueError(f"Key or event ID mismatch at offset {record.offset}")

        count = len(event["measurements"])
        print(f"offset {record.offset} | {key} | {day} | {count} measurements")
        measurement_count += count
        next_offset += 1
        if next_offset == end_offset:
            break
finally:
    consumer.close()

if next_offset != end_offset:
    raise RuntimeError(f"Read stopped early at offset {next_offset}")
if measurement_count != 26:
    raise ValueError(f"Expected 26 measurements, received {measurement_count}")

print(f"Verified {next_offset - start_offset} events and {measurement_count} measurements")