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


three_day_window = (
    daily.sort_values(["mw_connection_no", "score_date"])
    .set_index("score_date")
    .groupby("mw_connection_no")["max_error_second"]
    .rolling("3D", closed="right", min_periods=0)
)

three_day_features = three_day_window.agg(["count", "mean"]).rename(
    columns={"count": "days_with_kpi_3d", "mean": "avg_max_error_second_3d"}
)
three_day_features["days_with_kpi_3d"] = three_day_features["days_with_kpi_3d"].astype("int64")

daily = daily.merge(
    three_day_features.reset_index(),
    on=["mw_connection_no", "score_date"],
    how="left",
    validate="one_to_one",
)


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
        "days_with_kpi_3d", "avg_max_error_second_3d", "outcome_date", "failure_on_d1",
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

march_kpi = (
    daily.loc[daily["mw_connection_no"].eq("1,338,999")]
    .set_index("score_date")["max_error_second"]
    .sort_index()
)

# At date D, (D - 3 days, D] includes D-2, D-1, and D.
window_3d = march_kpi.rolling("3D", closed="right", min_periods=1)
march_check = pd.DataFrame({
    "days_with_kpi_3d": window_3d.count().astype("Int64"),
    "avg_max_error_second_3d": window_3d.mean(),
})

check_dates = pd.to_datetime(["2019-03-20", "2019-03-21", "2019-03-24", "2019-03-25"])
print("\nThree-day KPI check for 1,338,999")
print(march_check.loc[check_dates].to_string())

# Training outcomes in calendar year 2019 map to these scoring dates.
# Filter by score_date so eligible rows with an unknown D+1 outcome remain included.
training_score_dates = pd.date_range("2018-12-31", "2019-12-30", freq="D")

training_pool = reference.loc[
    reference["score_date"].between(
        training_score_dates[0], training_score_dates[-1]
    )
]

daily_eligible = (
    training_pool.groupby("score_date")["mw_connection_no"]
    .nunique()
    .reindex(training_score_dates, fill_value=0)
)

print("\nM5 · Checkpoint 1 — Training inspection pool")
print(f"Scoring dates: {len(daily_eligible)}")
print(f"Minimum eligible links per day: {daily_eligible.min()}")
print(f"Median eligible links per day: {daily_eligible.median():g}")
print(f"Maximum eligible links per day: {daily_eligible.max()}")
print(f"Days with fewer than 10 eligible links: {daily_eligible.lt(10).sum()}")

print("\nM5 · Checkpoint 2 — Reference fields")
for column in reference.columns:
    print(
        f"{column}: {reference[column].dtype}, "
        f"non-missing={reference[column].notna().sum()}, "
        f"missing={reference[column].isna().sum()}"
    )

print("\nM5 · Checkpoint 3 — Outcome coverage by year")
for year in sorted(reference["outcome_date"].dt.year.unique()):
    year_target = reference.loc[
        reference["outcome_date"].dt.year.eq(year), "failure_on_d1"
    ]

    print(
        f"{year}: rows={len(year_target)}, "
        f"known={year_target.notna().sum()}, "
        f"failures={year_target.eq(True).sum()}, "
        f"non-failures={year_target.eq(False).sum()}, "
        f"unknown={year_target.isna().sum()}"
    )

print("\nM5 · Checkpoint 4 — Chronological split coverage")
for year in (2019, 2020):
    split = reference.loc[reference["outcome_date"].dt.year.eq(year)]

    print(
        f"Outcome year {year}: "
        f"outcome dates {split['outcome_date'].min().date()} to "
        f"{split['outcome_date'].max().date()} "
        f"({split['outcome_date'].nunique()} distinct); "
        f"scoring dates {split['score_date'].min().date()} to "
        f"{split['score_date'].max().date()} "
        f"({split['score_date'].nunique()} distinct); "
        f"unknown labels={split['failure_on_d1'].isna().sum()}"
    )

backtest_features = [
    "max_error_second",
    "prev_day_max_error_second",
    "error_second_change_1d",
    "days_with_kpi_3d",
    "avg_max_error_second_3d",
]

training_rows = reference.loc[
    reference["outcome_date"].dt.year.eq(2019)
    & reference["failure_on_d1"].notna()
].copy()

test_pool = reference.loc[
    reference["outcome_date"].dt.year.eq(2020)
].copy()

test_known = test_pool.loc[test_pool["failure_on_d1"].notna()]

print("\nM5 · Checkpoint 5 — Backtest rows and feature coverage")
print(
    f"Training: rows={len(training_rows)}, "
    f"failures={training_rows['failure_on_d1'].eq(True).sum()}, "
    f"scoring dates={training_rows['score_date'].nunique()}"
)
print(
    f"2020 test pool: candidates={len(test_pool)}, "
    f"known outcomes={len(test_known)}, "
    f"failures={test_known['failure_on_d1'].eq(True).sum()}, "
    f"unknown outcomes={test_pool['failure_on_d1'].isna().sum()}, "
    f"scoring dates={test_pool['score_date'].nunique()}"
)

for column in backtest_features:
    print(
        f"{column}: training missing={training_rows[column].isna().sum()}, "
        f"test missing={test_pool[column].isna().sum()}"
    )

from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

model = make_pipeline(
    SimpleImputer(strategy="median", add_indicator=True),
    StandardScaler(),
    LogisticRegression(max_iter=1000),
)

model.fit(
    training_rows[backtest_features],
    training_rows["failure_on_d1"].astype("int8"),
)

test_scored = test_pool.copy()
test_scored["logistic_score"] = model.predict_proba(
    test_pool[backtest_features]
)[:, 1]

print("\nM5 · Checkpoint 6 — Logistic regression scoring")
print(f"Training rows fitted: {len(training_rows)}")
print(f"Training failures: {training_rows['failure_on_d1'].eq(True).sum()}")
print(f"2020 candidates scored: {len(test_scored)}")
print(f"Unknown outcomes retained in scoring pool: {test_scored['failure_on_d1'].isna().sum()}")
print(f"All logistic scores are present: {test_scored['logistic_score'].notna().all()}")
print(
    "Logistic score range: "
    f"{test_scored['logistic_score'].min():.6f} to "
    f"{test_scored['logistic_score'].max():.6f}"
)

from sklearn.metrics import average_precision_score

inspection_budget = 10
known_test = test_scored.loc[test_scored["failure_on_d1"].notna()]
total_known_failures = known_test["failure_on_d1"].eq(True).sum()

def select_daily_top_k(frame, score_column, k):
    return (
        frame.sort_values(
            ["score_date", score_column, "mw_connection_no"],
            ascending=[True, False, True],
        )
        .groupby("score_date", sort=True)
        .head(k)
    )

print("\nM5 · Checkpoint 7 — 2020 ranking comparison")
print(
    f"Inspection budget: up to {inspection_budget} per day; "
    f"scoring dates={test_scored['score_date'].nunique()}"
)
print(f"Known eligible failures for recall denominator: {total_known_failures}")

for method, score_column in [
    ("Recent-error baseline", "max_error_second"),
    ("Logistic regression", "logistic_score"),
]:
    selected = select_daily_top_k(test_scored, score_column, inspection_budget)
    known_selected = selected.loc[selected["failure_on_d1"].notna()]

    hits = known_selected["failure_on_d1"].eq(True).sum()
    false_alarms = known_selected["failure_on_d1"].eq(False).sum()
    unknown = selected["failure_on_d1"].isna().sum()
    precision = hits / len(known_selected) if len(known_selected) else 0
    recall = hits / total_known_failures

    average_precision = average_precision_score(
        known_test["failure_on_d1"].astype("int8"),
        known_test[score_column],
    )

    print(
        f"{method}: selected={len(selected)}, "
        f"known inspected={len(known_selected)}, "
        f"failures captured={hits}, false alarms={false_alarms}, "
        f"unknown outcomes={unknown}, precision={precision:.4%}, "
        f"recall={recall:.4%}, average precision={average_precision:.6f}"
    )

print("\nReplay fixture")
print(f"Saved {len(fixture)} scoring rows to {fixture_path}")
