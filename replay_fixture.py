from pathlib import Path
from zipfile import ZipFile
import json
import pandas as pd
import argparse
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--publish", action="store_true", help="Send the fixture to Kafka")
args = parser.parse_args()

fixture_path = Path("fixtures/daily_reference.tsv")
archive_path = Path("data/raw/RLF_Prediction_ITU_AIML_Challenge_Data/RegionA.zip")

keys = ["mw_connection_no", "datetime"]
scoring_days = pd.read_csv(
    fixture_path,
    sep="\t",
    usecols=["mw_connection_no", "score_date"],
    dtype={"mw_connection_no": str},
    parse_dates=["score_date"],
)

# Adjacent scoring days can require the same observation.
required_days = pd.concat(
    [
        scoring_days.assign(datetime=scoring_days["score_date"] + pd.Timedelta(days=offset))
        for offset in (-2, -1, 0, 1)
    ],
    ignore_index=True,
)[keys].drop_duplicates()

with ZipFile(archive_path) as archive, archive.open("rl-kpis.tsv") as source:
    kpis = pd.read_csv(
        source,
        sep="\t",
        usecols=[*keys, "mlid", "error_second", "rlf"],
        dtype=str,
        keep_default_na=False,
    )

kpis["datetime"] = pd.to_datetime(kpis["datetime"], format="%Y-%m-%d")
selected = kpis.merge(required_days, on=keys, how="inner", validate="many_to_one")
selected = selected.sort_values(["datetime", "mw_connection_no", "mlid"])

counts = selected.groupby(["datetime", "mw_connection_no"]).size()
print(f"Requested connection-days: {len(required_days)}")
print(f"Observed connection-days: {len(counts)}")
print(counts.rename("source_rows").to_string())

events = []
failure_flags = {"False": False, "True": True}


for (day, connection), rows in selected.groupby(
    ["datetime", "mw_connection_no"], sort=True
):
    observation_date = day.date().isoformat()
    measurements = []

    for row in rows.itertuples(index=False):
        error_text = row.error_second.strip()
        measurements.append({
            "mlid": row.mlid,
            "error_second": float(error_text) if error_text else None,
            "rlf": failure_flags[row.rlf],
        })

    event = {
        "schema_version": 1,
        "event_id": f"{connection}|{observation_date}",
        "mw_connection_no": connection,
        "observation_date": observation_date,
        "measurements": measurements,
    }
    events.append(event)

print(f"\nFixture preview: {len(events)} events, {len(selected)} source KPI rows")
example = next(event for event in events if event["event_id"] == "230,321|2019-01-02")
print(f"Kafka key: {example['mw_connection_no']}")
print(json.dumps(example, indent=2, allow_nan=False))

if args.publish:
    topic = "backhaul-link-kpis"
    producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092", acks="all")
    offsets = []

    try:
        for event in events:
            key = event["mw_connection_no"].encode("utf-8")
            value = json.dumps(event, allow_nan=False).encode("utf-8")
            receipt = producer.send(topic, key=key, value=value).get(timeout=30)
            offsets.append(receipt.offset)
            print(f"{event['event_id']} -> partition {receipt.partition}, offset {receipt.offset}")
    finally:
        producer.close()

    print(f"Published {len(offsets)} events to {topic}")
    print(f"Partition 0 offsets: [{offsets[0]}, {offsets[-1] + 1})")