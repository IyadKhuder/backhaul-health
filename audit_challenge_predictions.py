from zipfile import ZipFile
from io import BytesIO
from subprocess import check_output

import pandas as pd


archive = "data/raw/RLF_Prediction_ITU_AIML_Challenge_Test.7z"
for snapshot in ("20210426", "20210525", "20210614", "20210817"):
    member = f"test_{snapshot}/{snapshot}_predicts.tsv"
    content = check_output(["7z", "x", "-so", archive, member])
    rows = pd.read_csv(BytesIO(content), sep="\t", dtype=str, keep_default_na=False)

    dates = pd.to_datetime(rows["datetime"], format="%Y-%m-%d", errors="raise")
    print(f"{snapshot}: {len(rows):,} rows, {dates.min():%Y-%m-%d} to {dates.max():%Y-%m-%d}")
    for column in ("1-day-predict", "5-day-predict"):
        filled = rows[column].str.strip().ne("").sum()
        print(f"  {column}: {filled:,} populated")


template_dates = {
    "20210426": "2021-05-15",
    "20210525": "2021-06-13",
    "20210614": "2021-07-03",
    "20210817": "2021-09-05",
}
outcome_dates = pd.to_datetime(list(template_dates.values())) + pd.Timedelta(days=1)

print("\nKPI coverage across test snapshots")
for snapshot in template_dates:
    member = f"test_{snapshot}/RegionA_test_{snapshot}.zip"
    content = check_output(["7z", "x", "-so", archive, member])

    with ZipFile(BytesIO(content)) as region:
        with region.open("rl-kpis.tsv") as source:
            kpis = pd.read_csv(
                source, sep="\t", usecols=["datetime"], dtype=str
            )

    dates = pd.to_datetime(kpis["datetime"], format="%Y-%m-%d", errors="raise")
    counts = dates.value_counts().reindex(outcome_dates, fill_value=0)

    print(f"{snapshot}: {dates.min():%Y-%m-%d} to {dates.max():%Y-%m-%d}")
    print("  " + ", ".join(f"{day:%Y-%m-%d}={n:,}" for day, n in counts.items()))

dates = pd.to_datetime(kpis["datetime"], format="%Y-%m-%d", errors="raise")


def connection_status(snapshot, day):
    member = f"test_{snapshot}/RegionA_test_{snapshot}.zip"
    content = check_output(["7z", "x", "-so", archive, member])
    with ZipFile(BytesIO(content)) as region, region.open("rl-kpis.tsv") as source:
        rows = pd.read_csv(
            source,
            sep="\t",
            usecols=lambda name: name in {"datetime", "mw_connection_no", "rlf"},
            dtype=str,
            keep_default_na=False,
        )

    rows = rows.loc[rows["datetime"].eq(day)].copy()
    if "rlf" not in rows.columns:
        print(f"{day}: rlf column absent")
        rows["rlf"] = ""
    else:
        print(f"{day}: raw rlf counts {rows['rlf'].value_counts().to_dict()}")

    rows = rows.loc[rows["mw_connection_no"].str.strip().ne("")]
    rows["positive"] = rows["rlf"].eq("True")
    rows["clear"] = rows["rlf"].eq("False")
    grouped = rows.groupby("mw_connection_no")
    status = pd.Series("unknown", index=grouped.size().index)
    status.loc[grouped["clear"].all()] = "clear"
    status.loc[grouped["positive"].any()] = "positive"
    return status


print("\nJune 13 to June 14 connection outcomes")
on_d = connection_status("20210525", "2021-06-13")
on_d1 = connection_status("20210614", "2021-06-14")
eligible_ids = on_d.index[on_d.eq("clear")]
outcomes = on_d1.reindex(eligible_ids).fillna("unknown")

print(f"Clear connections on June 13: {len(eligible_ids):,}")
for status in ("positive", "clear", "unknown"):
    print(f"  June 14 {status}: {outcomes.eq(status).sum():,}")