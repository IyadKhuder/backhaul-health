# Local setup

Edit files in PyCharm on Windows. Run commands from Ubuntu under WSL.

## Python

From the project root, activate the project environment and install its dependencies:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Verified with Ubuntu 24.04.3 LTS and Python 3.12.3.

## Kafka

The existing Docker container `network-kafka` runs `apache/kafka:4.4.0`.
Python clients in Ubuntu connect to `127.0.0.1:9092`.

List topics on the running broker:

```bash
docker exec network-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

The `network-pings` topic belongs to the separate RIPE Atlas project.

## Flink SQL

Launch an interactive client using the local Flink 1.20.3 image with Java 17:

```bash
docker run --rm -it --pull=never flink:1.20.3-scala_2.12-java17 \
  /opt/flink/bin/sql-client.sh
```

At the `Flink SQL>` prompt, run this two-row batch query:

```sql
SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT COUNT(*) AS links_observed
FROM (VALUES ('link-A'), ('link-B')) AS sample(link_id);
```

Expected result: one row with `links_observed = 2`. Enter `QUIT;` to return to Ubuntu.