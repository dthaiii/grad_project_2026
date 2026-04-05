INSERT {{ BIGQUERY_DATASET }}.{{ USER_INFO_TABLE }}
SELECT
    ts,
    userId,
    firstName,
    lastName,
    gender,
    level,
    registration
FROM {{ BIGQUERY_DATASET }}.{{ USER_INFO_TABLE}}_{{ logical_date.strftime("%m%d%H") }}