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
    'scan.startup.specific-offsets' = 'partition:0,offset:0',
    'scan.bounded.mode' = 'specific-offsets',
    'scan.bounded.specific-offsets' = 'partition:0,offset:8'
);

SELECT
    COUNT(*) AS events_read,
    MIN(kafka_offset) AS first_offset,
    MAX(kafka_offset) AS last_offset,
    SUM(CARDINALITY(measurements)) AS measurement_rows
FROM fixture_events;