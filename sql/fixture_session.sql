ADD JAR '/connectors/flink-sql-connector-kafka-3.4.0-1.20.jar';

SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE fixture_events (
    event_id STRING,
    mw_connection_no STRING,
    observation_date DATE,
    measurements ARRAY<ROW<mlid STRING, error_second DOUBLE, rlf BOOLEAN>>,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'backhaul-link-kpis',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'backhaul-health-flink-fixture',
    'format' = 'json',
    'scan.startup.mode' = 'specific-offsets',
    'scan.startup.specific-offsets' = 'partition:0,offset:8',
    'scan.bounded.mode' = 'specific-offsets',
    'scan.bounded.specific-offsets' = 'partition:0,offset:17'
);

CREATE TEMPORARY VIEW fixture_daily AS
SELECT
    e.mw_connection_no,
    e.observation_date,
    COUNT(*) AS observed_rows,
    MAX(m.error_second) AS max_error_second,
    MAX(CASE WHEN m.rlf THEN 1 ELSE 0 END) = 1 AS failure_on_d
FROM fixture_events AS e
CROSS JOIN UNNEST(e.measurements) AS m(mlid, error_second, rlf)
GROUP BY e.mw_connection_no, e.observation_date;

CREATE TEMPORARY VIEW fixture_daily_features AS
SELECT
    d.mw_connection_no,
    d.observation_date AS score_date,
    d.observed_rows,
    d.max_error_second,
    p.max_error_second AS prev_day_max_error_second,
    d.max_error_second - p.max_error_second AS error_second_change_1d,
    d.failure_on_d
FROM fixture_daily AS d
LEFT JOIN fixture_daily AS p
    ON p.mw_connection_no = d.mw_connection_no
   AND p.observation_date = d.observation_date - INTERVAL '1' DAY;

CREATE TEMPORARY VIEW fixture_scored_outcomes AS
SELECT
    f.mw_connection_no,
    f.score_date,
    f.observed_rows,
    f.max_error_second,
    f.prev_day_max_error_second,
    f.error_second_change_1d,
    CAST(f.score_date + INTERVAL '1' DAY AS DATE) AS outcome_date,
    n.failure_on_d AS failure_on_d1
FROM fixture_daily_features AS f
LEFT JOIN fixture_daily AS n
    ON n.mw_connection_no = f.mw_connection_no
   AND n.observation_date = f.score_date + INTERVAL '1' DAY
WHERE f.failure_on_d = FALSE;

CREATE TEMPORARY VIEW fixture_three_day_features AS
SELECT
    d.mw_connection_no,
    d.observation_date AS score_date,
    COUNT(h.max_error_second) AS days_with_kpi_3d,
    AVG(h.max_error_second) AS avg_max_error_second_3d
FROM fixture_daily AS d
LEFT JOIN fixture_daily AS h
    ON h.mw_connection_no = d.mw_connection_no
   AND h.observation_date BETWEEN
       d.observation_date - INTERVAL '2' DAY AND d.observation_date
GROUP BY d.mw_connection_no, d.observation_date;

-- Read both fixture replays from partition 0, offsets [0, 17).
-- fixture_events already defines the exclusive end offset of 17.
CREATE TEMPORARY TABLE fixture_all_events
WITH (
    'scan.startup.specific-offsets' = 'partition:0,offset:0'
)
LIKE fixture_events (OVERWRITING OPTIONS);

-- Both replays contain the same source values for a repeated event ID.
-- Keep its earliest offset in this single-partition fixture.
-- Keep first arrivals even when their observation date is out of order.
-- Example: the 18 March event first arrived at offset 11 and contributes
-- to the completed 20 March feature window through its observation date.
CREATE TEMPORARY VIEW fixture_unique_events AS
SELECT
    event_id,
    mw_connection_no,
    observation_date,
    measurements,
    kafka_offset
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY kafka_offset
        ) AS copy_number
    FROM fixture_all_events AS e
) AS ranked
WHERE copy_number = 1;