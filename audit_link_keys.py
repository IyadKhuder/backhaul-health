from zipfile import ZipFile

import pandas as pd


archive = "data/raw/RLF_Prediction_ITU_AIML_Challenge_Data/RegionA.zip"
columns = ["datetime", "mw_connection_no", "tip", "mlid", "site_id", "rlf"]

with ZipFile(archive) as region, region.open("rl-kpis.tsv") as source:
    kpis = pd.read_csv(source, sep="\t", usecols=columns, dtype=str, keep_default_na=False)

dates = pd.to_datetime(kpis["datetime"], format="%Y-%m-%d", errors="coerce")
print(f"KPI rows: {len(kpis):,}")
print(f"Unparsed dates: {dates.isna().sum():,}")
print(f"Observed dates: {dates.nunique():,} ({dates.min():%Y-%m-%d} to {dates.max():%Y-%m-%d})")

candidates = [
    ["mw_connection_no"],
    ["mlid"],
    ["mw_connection_no", "tip"],
    ["mw_connection_no", "mlid"],
    ["mw_connection_no", "site_id"],
]

for fields in candidates:
    complete = dates.notna() & kpis[fields].ne("").all(axis=1)
    counts = kpis.loc[complete].groupby([*fields, "datetime"]).size()
    print(
        f"{' + '.join(fields)} | incomplete rows: {(~complete).sum():,}"
        f" | repeated key-days: {counts.gt(1).sum():,}"
        f" | max rows per key-day: {counts.max()}"
    )


print("\nIdentifier relationships across all observed dates")
pairs = kpis[["mlid", "mw_connection_no"]].drop_duplicates()
connections_per_mlid = pairs.groupby("mlid").size()

print(f"Distinct mlid values: {len(connections_per_mlid):,}")
print(f"Distinct connection numbers: {pairs['mw_connection_no'].nunique():,}")
print(f"mlid values associated with multiple connections: {(connections_per_mlid > 1).sum():,}")

day_key = ["mw_connection_no", "datetime"]
rows_per_day = kpis.groupby(day_key).size()
print("\nKPI rows per connection-day")
print(rows_per_day.value_counts().sort_index().rename("connection_days").to_string())

connection, day = rows_per_day.idxmax()
example = kpis.loc[
    kpis["mw_connection_no"].eq(connection) & kpis["datetime"].eq(day),
    ["mw_connection_no", "datetime", "mlid", "tip", "site_id"],
]
print("\nExample connection-day with the most KPI rows")
print(example.sort_values(["tip", "mlid"]).to_string(index=False))


print("\nRaw rlf values and row counts")
for value, count in kpis["rlf"].value_counts(dropna=False).items():
    print(f"  {value!r}: {count:,}")

label_flags = kpis[day_key].copy()
label_flags["has_true"] = kpis["rlf"].eq("True")
label_flags["has_false"] = kpis["rlf"].eq("False")
label_flags["has_other"] = ~kpis["rlf"].isin(["True", "False"])

daily_flags = label_flags.groupby(day_key).any()
patterns = daily_flags.value_counts().rename("connection_days")

print("\nLabel combinations within a connection-day")
print(patterns.to_string())
print(f"Total connection-days: {len(daily_flags):,}")


daily = daily_flags["has_true"].rename("failure").reset_index()
daily["datetime"] = pd.to_datetime(daily["datetime"], format="%Y-%m-%d")
daily = daily.sort_values(["mw_connection_no", "datetime"])

by_connection = daily.groupby("mw_connection_no")
next_date = by_connection["datetime"].shift(-1)
next_failure = by_connection["failure"].shift(-1)
consecutive = next_date.eq(daily["datetime"] + pd.Timedelta(days=1))

eligible = ~daily["failure"] & consecutive
new_failures = (eligible & next_failure.eq(True)).sum()
stayed_clear = (eligible & next_failure.eq(False)).sum()

print("\nNext-day feasibility")
print(f"Observed connection-days: {len(daily):,}")
print(f"Already positive on D: {daily['failure'].sum():,}")
print(f"Clear on D, no observation on D+1: {(~daily['failure'] & ~consecutive).sum():,}")
print(f"Eligible consecutive pairs: {eligible.sum():,}")
print(f"  Clear on D, positive on D+1: {new_failures:,}")
print(f"  Clear on D, clear on D+1: {stayed_clear:,}")


outcomes = pd.DataFrame({
    "outcome_month": next_date.loc[eligible].dt.to_period("M"),
    "new_failure": next_failure.loc[eligible].astype(bool),
})

monthly = outcomes.groupby("outcome_month").agg(
    eligible_pairs=("new_failure", "size"),
    new_failures=("new_failure", "sum"),
)
monthly["stayed_clear"] = monthly["eligible_pairs"] - monthly["new_failures"]

print("\nNext-day outcomes by outcome month")
print(monthly.to_string())
print("\nTotals")
print(monthly.sum().to_string())


pairs = daily.loc[eligible, ["mw_connection_no"]].copy()
pairs["outcome_date"] = next_date.loc[eligible]
pairs["new_failure"] = next_failure.loc[eligible].astype(bool)

periods = [
    ("Training", "2019-01-01", "2019-12-31"),
    ("Validation", "2020-01-01", "2020-06-30"),
    ("Holdout", "2020-07-01", "2020-12-25"),
]

print("\nFailure coverage by outcome date")
for name, start, end in periods:
    period = pairs.loc[pairs["outcome_date"].between(start, end)]
    failures = period.loc[period["new_failure"]]
    counts_by_date = failures.groupby("outcome_date").size()
    peak_date = counts_by_date.idxmax()

    print(
        f"{name}: {len(failures):,} failures across "
        f"{len(counts_by_date)} of {period['outcome_date'].nunique()} evaluable dates "
        f"and {failures['mw_connection_no'].nunique()} distinct connections. "
        f"Peak: {peak_date:%Y-%m-%d} ({counts_by_date.max()} failures)."
    )


training_failures = pairs.loc[
    pairs["new_failure"] & pairs["outcome_date"].dt.year.eq(2019)
]
example = training_failures.sort_values(
    ["outcome_date", "mw_connection_no"]
).iloc[0]

connection = example["mw_connection_no"]
outcome_day = example["outcome_date"]
score_day = outcome_day - pd.Timedelta(days=1)

trace = kpis.loc[
    kpis["mw_connection_no"].eq(connection)
    & dates.isin([score_day, outcome_day]),
    ["datetime", "mw_connection_no", "mlid", "tip", "site_id", "rlf"],
].sort_values(["datetime", "tip", "mlid"])

print("\nTraining example: clear on D, recorded failure on D+1")
print(trace.to_string(index=False))