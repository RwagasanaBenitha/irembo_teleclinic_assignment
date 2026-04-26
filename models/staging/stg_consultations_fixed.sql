-- stg_consultations_fixed.sql
-- Fixes two known data quality issues in the raw consultations table:
--   1. Timezone bug: ~34% of April records from new app version carry UTC+2 offset
--      in the consultation_started_at string. These are normalised to UTC so that
--      wait_time_minutes is computed correctly.
--   2. Test/seed accounts: rows where patient_id begins with 'TEST_' are excluded.
--
-- Implausibility threshold (see wait_time_minutes below):
--   • Negative values             → NULL  (physically impossible)
--   • Values > 480 minutes (8 h) → NULL  (beyond any plausible clinical queue;
--     consults are scheduled same-day on the TeleClinic platform)

WITH raw AS (
    SELECT *
    FROM {{ source('teleclinic_raw', 'consultations') }}
    -- Exclude test / seed accounts
    WHERE patient_id NOT LIKE 'TEST_%'
),

tz_classified AS (
    SELECT
        *,
        -- Detect records whose timestamp string contains a UTC+2 offset marker.
        -- The new app version writes ISO-8601 strings ending in '+02:00' or '+0200'.
        -- ClickHouse: positionCaseInsensitive returns 0 when the substring is absent.
        (
            positionCaseInsensitive(toString(consultation_started_at), '+02:00') > 0
            OR positionCaseInsensitive(toString(consultation_started_at), '+0200') > 0
        ) AS _has_tz_offset
    FROM raw
),

normalised AS (
    SELECT
        consultation_id,
        patient_id,
        provider_id,
        consultation_created_at,

        -- Normalise started_at to UTC.
        -- For records with a UTC+2 offset, toTimeZone() re-interprets the stored
        -- instant correctly, then we cast back to a naive UTC DateTime.
        -- For records already in UTC we leave the value untouched.
        CASE
            WHEN _has_tz_offset
                THEN toDateTime(toTimeZone(toDateTime64(toString(consultation_started_at), 3, 'Etc/GMT-2'), 'UTC'))
            ELSE toDateTime(consultation_started_at)
        END AS consultation_started_at_utc,

        _has_tz_offset AS is_tz_corrected,

        -- Preserve any other columns from raw that downstream models need
        status,
        channel,          -- 'app' | 'ussd'
        app_version
    FROM tz_classified
),

with_wait_time AS (
    SELECT
        *,

        -- Raw difference in minutes between request creation and doctor pick-up.
        dateDiff('minute', consultation_created_at, consultation_started_at_utc) AS _raw_wait_mins,

        -- Clean wait time: NULL for negative or implausibly large values.
        -- Threshold: > 480 minutes (8 hours) is considered implausible.
        CASE
            WHEN dateDiff('minute', consultation_created_at, consultation_started_at_utc) < 0   THEN NULL
            WHEN dateDiff('minute', consultation_created_at, consultation_started_at_utc) > 480  THEN NULL
            ELSE dateDiff('minute', consultation_created_at, consultation_started_at_utc)
        END AS wait_time_minutes

    FROM normalised
)

SELECT
    consultation_id,
    patient_id,
    provider_id,
    consultation_created_at,
    consultation_started_at_utc,
    wait_time_minutes,
    is_tz_corrected,
    status,
    channel,
    app_version
FROM with_wait_time
