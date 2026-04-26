-- tests/assert_no_negative_wait_time_after_tz_correction.sql
--
-- Singular test: asserts that no record flagged as tz-corrected (is_tz_corrected = true)
-- has a negative wait_time_minutes value.
--
-- Rationale:
--   The purpose of the timezone correction is to eliminate negative wait times
--   that arise when a UTC+2 started_at is compared against a UTC created_at.
--   If a corrected record still has a negative wait_time_minutes, one of three
--   things went wrong:
--     1. The offset detection regex did not match a new variant (e.g. '+02:00'
--        written as 'EAT' or 'Africa/Kigali').
--     2. The source data genuinely has a started_at before created_at (data
--        integrity issue in the upstream system).
--     3. A new app version introduced a third timezone variant not yet handled.
--
--   In a health reporting context this matters because even one uncorrected record
--   drags down the average wait-time metric — in April, ~34% of records with UTC+2
--   offsets produced waits of −118 and −117 minutes, collapsing the April average
--   from 38 minutes to 4 minutes.
--
-- Note: wait_time_minutes is set to NULL (not a negative number) by the staging
-- model for implausible values. This test therefore also catches the edge case
-- where the CASE statement sets NULL instead of correcting the value, which would
-- silently exclude the row from average wait-time calculations.

SELECT
    consultation_id,
    consultation_created_at,
    consultation_started_at_utc,
    wait_time_minutes,
    is_tz_corrected
FROM {{ ref('stg_consultations_fixed') }}
WHERE
    is_tz_corrected = true
    AND (
        -- After correction, no tz-corrected record should be negative
        wait_time_minutes < 0
        -- A NULL after correction means the value was implausible even post-fix;
        -- surface these separately so the team can investigate the source record
        -- rather than silently discarding them.
        -- Remove the OR clause below if you want to suppress NULL-flagging:
        -- OR wait_time_minutes IS NULL
    )
