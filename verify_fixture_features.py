import json
from math import isclose

import pandas as pd
from kafka import KafkaConsumer, TopicPartition


keys = ["mw_connection_no", "score_date"]
counts = ["observed_rows", "days_with_kpi_3d"]
metrics = [
    "max_error_second",
    "prev_day_max_error_second",
    "error_second_change_1d",
    "avg_max_error_second_3d",
]
expected = pd.read_csv(
    "fixtures/daily_reference.tsv",
    sep="\t",
    dtype={column: str for column in keys},
).set_index(keys, verify_integrity=True)

partition = TopicPartition("backhaul-link-features", 0)
consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id=None,
    enable_auto_commit=False,
    auto_offset_reset="none",
    consumer_timeout_ms=10_000,
)
seen = set()

try:
    consumer.assign([partition])
    consumer.seek(partition, 0)

    for offset in range(4):
        record = next(consumer, None)
        if record is None or record.offset != offset:
            raise ValueError(f"Missing expected Kafka offset {offset}")

        row = json.loads(record.value)
        if set(row) != set(keys + counts + metrics):
            raise ValueError(f"Unexpected feature schema at offset {offset}: {sorted(row)}")

        key = (row["mw_connection_no"], row["score_date"])
        if record.key != key[0].encode("utf-8"):
            raise ValueError(f"Kafka key disagrees with payload at offset {offset}")
        if key in seen or key not in expected.index:
            raise ValueError(f"Duplicate or unexpected scoring row: {key}")

        reference = expected.loc[key, :]
        for column in counts:
            if type(row[column]) is not int or row[column] != reference[column]:
                raise ValueError(f"{key} {column}: got {row[column]}, expected {reference[column]}")

        for column in metrics:
            actual = row[column]
            target = reference[column]
            if pd.isna(target):
                matches = actual is None
            else:
                matches = type(actual) in (int, float) and isclose(
                    actual, target, rel_tol=0.0, abs_tol=1e-9
                )
            if not matches:
                raise ValueError(f"{key} {column}: got {actual}, expected {target}")

        seen.add(key)
finally:
    consumer.close()

if seen != set(expected.index):
    raise ValueError(f"Missing scoring rows: {set(expected.index) - seen}")

print("Verified 4 Kafka feature records against pandas at partition 0 offsets [0, 4)")