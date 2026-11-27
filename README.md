# backhaul-health

**Predicting Mobile Backhaul Link Failures with Kafka and Flink**

At the end of each day, a network operations engineer can inspect only a limited number of microwave backhaul links. This project asks which links to prioritize for inspection because they are at risk of a recorded radio-link failure on the following calendar day.

**Project status — 23 November 2026.** The historical replay and feature pipeline are working. Python publishes daily observations to Kafka, Flink SQL calculates features and publishes them to a second topic, and a Python consumer verifies those features against an independent pandas reference. The next milestone is a chronological backtest comparing an engineering baseline with logistic regression. Model performance and the Elasticsearch/Kibana demo are still to come.

## Data and prediction target

The project uses Turkcell’s public Region A data from the [ITU AI/ML in 5G Challenge](https://github.com/Turkcell/ITU-AIMLin5GChallenge-2021). The historical KPI table contains 1,992,986 measurements across 726 observed dates, from 31 December 2018 through 25 December 2020.

The inspection unit is a connection (`mw_connection_no`). A connection can have several measurement rows on the same day. For example, on 2 January 2019, connection `230,321` had two measurements. One reported a radio-link failure (`rlf = True`) and the other reported `False`. The connection therefore had a recorded failure that day.

A connection enters the scoring pool when it is observed on day D and all its failure flags are clear. Its outcome is positive if any measurement reports a failure on D+1, clear if it is observed with all flags clear, and unknown if the connection has no observation on that calendar day.

| Region A audit | Connection-days |
| --- | ---: |
| Clear on D and eligible for scoring | 959,307 |
| Recorded failure on D+1 | 886 |
| Observed clear on D+1 | 947,070 |
| D+1 outcome unknown | 11,351 |

## Working pipeline

1. **Python → Kafka.** The producer reads historical KPI measurements and publishes one JSON event per observed connection-day to `backhaul-link-kpis`. The Kafka key is the connection number. Each event carries its observation date, a deterministic event ID, and its individual measurements.
2. **Kafka → Flink SQL.** Flink reads a bounded range of Kafka offsets, expands the measurements, and calculates daily features. These include the daily maximum error seconds, the change from the previous calendar day, and the mean daily maximum over a three-calendar-day window. A coverage count records how many days contributed to that mean.
3. **Flink SQL → Kafka → Python.** Flink publishes scoring features to `backhaul-link-features`. A Python consumer matches each record to the pandas reference by connection and scoring date, checking the Kafka key, counts, numeric values, and missing values. Next-day outcomes are kept separate from the scoring messages.

## Verified replay

The expanded fixture contains **9 connection-day events and 30 underlying measurements**. It produces **4 scoring records** covering a next-day failure, clear outcomes, and a missing outcome.

The fixture also tests what a gap means in practice. Connection `1,338,999` was observed on 21 March and then on 24 March 2019. The 21 March score has an unknown next-day outcome. The 24 March score has a known clear next-day outcome, but its previous-day feature is missing because there was no observation on 23 March.

Across two replay runs, Flink read 17 Kafka records and retained 9 distinct events. The bounded calculation kept an 18 March observation that arrived after the 25 March observation and assigned it to the correct calendar window. All four published feature records matched pandas.

## Planned evaluation and demo

The backtest will compare a recent-error engineering ranking with logistic regression under the same daily inspection budget. Training uses 2019 outcome dates, validation uses January through June 2020, and the final holdout uses July through December 2020. The first evaluation step is to measure the daily training pool and choose an inspection budget.

The comparison will report how many recorded failures each queue captures, how many inspections have clear outcomes, and how many outcomes are unknown. The planned Elasticsearch and Kibana demo will show the queue as it stood at the end of D and reveal the recorded outcomes when the replay reaches D+1.

### M5 chronological backtest

The model was fitted on known outcomes dated in 2019 and evaluated on the
partial-year 2020 holdout (1 January–26 December outcomes; 361 scoring dates).
At up to 10 inspections per day, both methods ranked the full eligible pool,
including candidates whose next-day outcome was unknown. Precision and recall
use only candidates with known outcomes; unknown selected outcomes are reported
separately.

| Method | Selected | Known inspected | Failures captured | False alarms | Unknown outcomes | Missed failures | Precision | Recall | Average precision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Recent-error baseline (`max_error_second`) | 3,610 | 3,525 | 4 | 3,521 | 85 | 385 | 0.1135% | 1.0283% | 0.001082 |
| Logistic regression | 3,610 | 3,528 | 4 | 3,524 | 82 | 385 | 0.1134% | 1.0283% | 0.000881 |

The recent-error baseline had higher average precision in this holdout, while
both methods captured the same four failures at the 10-per-day budget. The
results do not show a useful improvement from logistic regression. Failures
were rare among known outcomes (389 of 464,461, or about 0.084%), and unknown
outcomes among selected candidates were not counted as either hits or false
alarms.

## Repository guide

| File | Purpose |
| --- | --- |
| [Local setup](docs/local-setup.md) | Python environment, Kafka broker, Flink SQL client, and connector setup |
| [Data feasibility audit](docs/data-feasibility.md) | Connection identifiers, calendar coverage, failure labels, and chronological splits |
| [Original fixture replay](docs/fixture-replay.md) | First eight-event replay and its verified Kafka offset range |
| [Pandas reference](build_daily_reference.py) | Build connection-day features and the expected fixture rows |
| [Replay producer](replay_fixture.py) | Preview or publish the historical fixture |
| [Flink session SQL](sql/fixture_session.sql) | Define the bounded Kafka source and feature views |
| [Flink feature publisher](sql/publish_fixture_features.sql) | Publish scoring features to Kafka |
| [Feature verification](verify_fixture_features.py) | Compare the published records with the pandas fixture |

## Data attribution

The dataset was introduced by Semih Aktaş, Hande Alemdar, and Salih Ergüt in *Towards 5G and beyond radio link diagnosis: Radio link failure prediction by using historical weather, link parameters*, Computers & Electrical Engineering, 2022. [Paper DOI](https://doi.org/10.1016/j.compeleceng.2022.107742)
