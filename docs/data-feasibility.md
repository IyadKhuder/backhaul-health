# Region A next-day failure feasibility

## Source

Turkcell ITU challenge repository, revision `4938c53979d650f94d5208ff4cb977ea954b6907`.

| Archive | SHA-256 |
|---|---|
| RLF_Prediction_ITU_AIML_Challenge_Data.7z | `b10ba1f8d298fa8d19e1cc8cfec65ea7556217bf4af90566d673b0e3834cb74c` |
| RLF_Prediction_ITU_AIML_Challenge_Test.7z | `c9cc9ecefe31b5eb7a16a06c6783cde70d2a40278521877a46752d13315a316a` |

## Inspection unit and target

The main KPI table contains 1,992,986 rows on 726 dates from 2018-12-31 through 2020-12-25. The pair `(mlid, datetime)` uniquely identifies a measurement row. A connection can have up to four rows per day, so the inspection queue uses one entry per `mw_connection_no` and scoring date D.

A connection is eligible at the end of D when all its observed `rlf` values are False. Its D+1 outcome is positive if any observed row for that connection records True on the next calendar day. If all observed rows are False, the outcome is clear. A missing D+1 observation gives an unknown outcome.

For example, both ends of connection `230,321` were clear on 2019-01-01. On 2019-01-02 the FAR row recorded True and the NEAR row recorded False. That counts as one new recorded connection failure.

## Chronological evaluation

Of 960,234 observed connection-days, 927 were already positive on D and 11,351 clear connection-days lacked a D+1 observation. The remaining 947,956 pairs contain 886 new recorded failures.

| Split by D+1 outcome date | Evaluable pairs | New failures | Stayed clear |
|---|---:|---:|---:|
| Training, Jan–Dec 2019 | 483,495 | 497 | 482,998 |
| Validation, Jan–Jun 2020 | 244,838 | 126 | 244,712 |
| Holdout, Jul 1–Dec 25 2020 | 219,623 | 263 | 219,360 |

Holdout failures occurred on 61 of 178 evaluable dates. October 8 accounted for 96 of the 263 failures, so the evaluation should show how that day affects aggregate results.

## Challenge snapshots

All four prediction templates have empty one-day and five-day prediction fields. Of their four scoring dates, only June 13, 2021 has next-calendar-day KPI observations in the published snapshots.

Of 1,151 clear connections on June 13, the June 14 KPI records identify 21 positive and 1,118 clear outcomes. The other 12 are unknown. This date pair provides a separate replay example with observed outcomes.