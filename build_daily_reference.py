from zipfile import ZipFile
from pathlib import Path
import pandas as pd


archive = "data/raw/RLF_Prediction_ITU_AIML_Challenge_Data/RegionA.zip"

with ZipFile(archive) as region, region.open("rl-kpis.tsv") as source:
    kpis = pd.read_csv(
        source,
        sep="\t",
        usecols=["mw_connection_no", "datetime", "rlf", "error_second"],
        dtype=str,
        keep_default_na=False,
    )

labels = kpis["rlf"].map({"False": False, "True": True})
if labels.isna().any():
    raise ValueError("Unexpected rlf value in the Region A KPI table.")

kpis["positive"] = labels
kpis["score_date"] = pd.to_datetime(kpis["datetime"], format="%Y-%m-%d")

error_seconds = kpis["error_second"].str.strip().replace("", pd.NA)
kpis["error_second"] = pd.to_numeric(error_seconds, errors="raise")

daily = (
    kpis.groupby(["mw_connection_no", "score_date"], as_index=False)
    .agg(
        observed_rows=("positive", "size"),
        positive_rows=("positive", "sum"),
        max_error_second=("error_second", "max"),
    )
)
daily["failure_on_d"] = daily["positive_rows"].gt(0)


previous_day = daily[["mw_connection_no", "score_date", "max_error_second"]].copy()
previous_day["score_date"] = previous_day["score_date"] + pd.Timedelta(days=1)
previous_day = previous_day.rename(columns={"max_error_second": "prev_day_max_error_second"})

daily = daily.merge(
    previous_day,
    on=["mw_connection_no", "score_date"],
    how="left",
    validate="one_to_one",
)

daily["error_second_change_1d"] = (
    daily["max_error_second"] - daily["prev_day_max_error_second"]
)

history = daily.sort_values(["mw_connection_no", "score_date"]).copy()
history["last_observed_date"] = history.groupby("mw_connection_no")["score_date"].shift()
history["days_since_observation"] = (
    history["score_date"] - history["last_observed_date"]
).dt.days

gap_example = history.loc[
    history["days_since_observation"].gt(1)
    & history["score_date"].dt.year.eq(2019)
    & ~history["failure_on_d"]
    & history["max_error_second"].notna(),
    [
        "mw_connection_no",
        "last_observed_date",
        "score_date",
        "days_since_observation",
        "max_error_second",
        "prev_day_max_error_second",
        "error_second_change_1d",
    ],
].head(1)


reference = daily.loc[~daily["failure_on_d"]].copy()
reference["outcome_date"] = reference["score_date"] + pd.Timedelta(days=1)

outcomes = daily[["mw_connection_no", "score_date", "failure_on_d"]].rename(
    columns={"score_date": "outcome_date", "failure_on_d": "failure_on_d1"}
)
outcomes["failure_on_d1"] = outcomes["failure_on_d1"].astype("boolean")

reference = reference.merge(
    outcomes,
    on=["mw_connection_no", "outcome_date"],
    how="left",
    validate="one_to_one",
)
gap_window = reference.loc[
    reference["mw_connection_no"].eq("1,338,999")
    & reference["score_date"].between("2019-03-20", "2019-03-25"),
    ["score_date", "outcome_date", "observed_rows", "failure_on_d1"],
].sort_values("score_date")


fixture_keys = pd.DataFrame(
    [
        ("230,321", "2019-01-01"),
        ("1,338,999", "2019-03-20"),
        ("1,338,999", "2019-03-21"),
        ("1,338,999", "2019-03-24"),
    ],
    columns=["mw_connection_no", "score_date"],
)
fixture_keys["score_date"] = pd.to_datetime(fixture_keys["score_date"])

fixture_columns = [
    "mw_connection_no", "score_date", "observed_rows",
    "max_error_second", "prev_day_max_error_second", "error_second_change_1d",
    "outcome_date", "failure_on_d1",
]
fixture = fixture_keys.merge(
    reference[fixture_columns],
    on=["mw_connection_no", "score_date"],
    how="left",
    validate="one_to_one",
)
if fixture["observed_rows"].isna().any():
    raise ValueError("A selected scoring day is missing from the pandas reference")

fixture_path = Path("fixtures/daily_reference.tsv")
fixture_path.parent.mkdir(exist_ok=True)
fixture.to_csv(fixture_path, sep="\t", index=False, na_rep="NA")

target = reference["failure_on_d1"]

connection = "230,321"
score_day = pd.Timestamp("2019-01-01")

raw_values = kpis.loc[
    kpis["mw_connection_no"].eq(connection) & kpis["score_date"].eq(score_day),
    "error_second",
]
example = reference.loc[
    reference["mw_connection_no"].eq(connection) & reference["score_date"].eq(score_day),
    ["observed_rows", "max_error_second", "failure_on_d1"],
]


window = daily.loc[
    daily["mw_connection_no"].eq("230,321")
    & daily["score_date"].between("2019-01-01", "2019-01-02"),
    ["score_date", "max_error_second", "prev_day_max_error_second", "error_second_change_1d", "failure_on_d"],
]

print("Connection-day totals")
print(f"Connection-days: {len(daily):,}")
print(f"KPI rows accounted for: {daily['observed_rows'].sum():,}")
print(f"Positive KPI rows: {daily['positive_rows'].sum():,}")
print(f"Positive connection-days: {daily['failure_on_d'].sum():,}")

print("\nNext-day outcomes for clear connections")
print(f"Clear connection-days scored: {len(reference):,}")
print(f"D+1 positive: {target.eq(True).sum():,}")
print(f"D+1 clear: {target.eq(False).sum():,}")
print(f"D+1 unknown: {target.isna().sum():,}")

print("\nDay-D feature check for 230,321 on 2019-01-01")
print("Source error_second values:", raw_values.tolist())
print(example.to_string(index=False))

print("\nPrevious-day feature check for 230,321")
print(window.sort_values("score_date").to_string(index=False))

print("\nCalendar-gap feature check")
print(gap_example.to_string(index=False))

print("\nNext-day outcomes around the March gap")
print(gap_window.to_string(index=False))

print("\nReplay fixture")
print(f"Saved {len(fixture)} scoring rows to {fixture_path}")
