# Kafka fixture replay

Each Kafka record contains one observed connection-day. The UTF-8 key is
`mw_connection_no`. The JSON value contains `schema_version`, `event_id`,
`mw_connection_no`, `observation_date`, and the individual `measurements`.
The event ID uses the format `mw_connection_no|observation_date`.

## Verified run

| Topic | Partition | Start offset (inclusive) | End offset (exclusive) | Events | Measurements |
| --- | ---: | ---: | ---: | ---: | ---: |
| `backhaul-link-kpis` | 0 | 0 | 8 | 8 | 26 |

Offset 1 contains the 1 January scoring day for `230,321`. Offset 2
contains the 2 January observation that reveals its recorded failure.
For `1,338,999`, offsets 5 and 6 correspond to 21 and 24 March.
The two intervening calendar days have no observations.

## Recheck the run

From the project root in Ubuntu under WSL, run:

```bash
.venv/bin/python verify_fixture_replay.py
```

Expected final line: `Verified 8 events and 26 measurements`.

Running `replay_fixture.py` without arguments previews the fixture.
Adding `--publish` appends another replay with new offsets.