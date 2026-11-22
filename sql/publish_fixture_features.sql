SET 'table.dml-sync' = 'true';

CREATE TEMPORARY TABLE fixture_feature_sink (
    mw_connection_no STRING,
    score_date DATE,
    observed_rows BIGINT,
    max_error_second DOUBLE,
    prev_day_max_error_second DOUBLE,
    error_second_change_1d DOUBLE,
    days_with_kpi_3d BIGINT,
    avg_max_error_second_3d DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'backhaul-link-features',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'raw',
    'key.fields' = 'mw_connection_no',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

INSERT INTO fixture_feature_sink
WITH daily AS (
    SELECT
        e.mw_connection_no,
        e.observation_date,
        COUNT(*) AS observed_rows,
        MAX(m.error_second) AS max_error_second,
        MAX(CASE WHEN m.rlf THEN 1 ELSE 0 END) AS failed
    FROM fixture_unique_events AS e
    CROSS JOIN UNNEST(e.measurements) AS m(mlid, error_second, rlf)
    GROUP BY e.mw_connection_no, e.observation_date
),
features AS (
    SELECT
        d.mw_connection_no,
        d.observation_date AS score_date,
        d.observed_rows,
        d.max_error_second,
        MAX(CASE WHEN h.observation_date = d.observation_date - INTERVAL '1' DAY
                 THEN h.max_error_second END) AS prev_day_max_error_second,
        COUNT(h.max_error_second) AS days_with_kpi_3d,
        AVG(h.max_error_second) AS avg_max_error_second_3d
    FROM daily AS d
    JOIN daily AS h
      ON h.mw_connection_no = d.mw_connection_no
     AND h.observation_date BETWEEN
         d.observation_date - INTERVAL '2' DAY AND d.observation_date
    WHERE d.failed = 0
      AND (
          (d.mw_connection_no = '230,321' AND d.observation_date = DATE '2019-01-01')
          OR
          (d.mw_connection_no = '1,338,999' AND d.observation_date IN (
              DATE '2019-03-20', DATE '2019-03-21', DATE '2019-03-24'
          ))
      )
    GROUP BY d.mw_connection_no, d.observation_date, d.observed_rows, d.max_error_second
)
SELECT
    mw_connection_no,
    score_date,
    observed_rows,
    max_error_second,
    prev_day_max_error_second,
    max_error_second - prev_day_max_error_second,
    days_with_kpi_3d,
    avg_max_error_second_3d
FROM features;