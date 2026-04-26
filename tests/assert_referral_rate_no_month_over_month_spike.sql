-- tests/assert_referral_rate_no_month_over_month_spike.sql
--
-- Singular test: detects an implausibly large month-over-month change in the
-- CLINICAL referral rate (doctor_referral_rate_pct).
--
-- Rationale:
--   The April anomaly was a jump from ~11% to 28% — a 17 percentage-point rise
--   in a single month. No genuine clinical event produces a swing of that
--   magnitude without a corresponding operational alert (e.g. an epidemic, a
--   policy change). A threshold of 8 percentage points (roughly 70% relative
--   increase from a baseline of ~11%) is conservative enough to catch the
--   April error while tolerating real but smaller seasonal fluctuations.
--
--   This test runs against mart_referral_rate_monthly after each dbt build.
--   Returning ANY rows causes the test to FAIL, blocking dashboard publication.
--
-- Adjust the threshold constant (8.0) if the platform's clinical baseline
-- shifts significantly — document the change and get sign-off from Clinical
-- Governance before updating.

WITH monthly_rates AS (
    SELECT
        consult_month,
        doctor_referral_rate_pct,
        -- Previous month's rate using ClickHouse window functions
        lagInFrame(doctor_referral_rate_pct, 1)
            OVER (ORDER BY consult_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS prev_month_rate_pct
    FROM {{ ref('mart_referral_rate_monthly') }}
),

anomalies AS (
    SELECT
        consult_month,
        prev_month_rate_pct,
        doctor_referral_rate_pct AS current_month_rate_pct,
        abs(doctor_referral_rate_pct - prev_month_rate_pct) AS absolute_change_pct
    FROM monthly_rates
    WHERE
        -- Only evaluate when we have a prior month to compare against
        prev_month_rate_pct IS NOT NULL
        -- Flag any month where the clinical referral rate shifts by more than
        -- 8 percentage points vs the prior month
        AND abs(doctor_referral_rate_pct - prev_month_rate_pct) > 8.0
)

-- Returning rows = test failure.
-- Each returned row represents a month that requires human investigation
-- before the dashboard can be published to external stakeholders.
SELECT *
FROM anomalies
