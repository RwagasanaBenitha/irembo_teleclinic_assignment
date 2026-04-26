-- mart_referral_rate_monthly.sql
-- Monthly referral rate mart: produces comparable Feb–Apr (and ongoing) figures
-- by separating doctor-issued referrals from patient-requested-only referrals.
--
-- The mart exposes THREE rate columns so consumers can choose the right metric:
--
--   doctor_referral_rate       → (doctor_referral + both) / total_consults
--                                The CLINICAL metric. Measures how often a qualified
--                                clinician judged a referral to be necessary.
--                                Comparable across all months including pre-April.
--
--   patient_requested_rate     → (patient_requested_only + both) / total_consults
--                                Measures patient-expressed demand. Only meaningful
--                                from 2025-04-03 (intake checkbox launch date).
--
--   combined_any_referral_rate → any referral signal / total_consults
--                                The definition the April dashboard accidentally used.
--                                Retained for auditability / trend-break documentation.
--
-- Handling the 'both' case:
--   A 'both' record is counted in BOTH doctor_referral_rate and patient_requested_rate
--   because both signals fired. It is counted ONCE in combined_any_referral_rate.
--   This means doctor_referral_rate + patient_requested_rate can exceed
--   combined_any_referral_rate — that is intentional and explained in the description.

SELECT
    consult_month,

    -- Volume
    count(*)                                                            AS total_consults,

    -- Referral counts by class (useful for absolute trend monitoring)
    countIf(referral_class = 'doctor_referral')                         AS doctor_referral_count,
    countIf(referral_class = 'patient_requested_only')                  AS patient_requested_only_count,
    countIf(referral_class = 'both')                                    AS both_count,
    countIf(referral_class = 'no_referral')                             AS no_referral_count,

    -- ── Rates ───────────────────────────────────────────────────────────────

    -- CLINICAL referral rate: what the doctor decided.
    -- 'both' is included because the doctor still formally issued the referral.
    round(
        countIf(referral_class IN ('doctor_referral', 'both'))
        / count(*) * 100,
        1
    )                                                                   AS doctor_referral_rate_pct,

    -- Patient-demand rate: how often patients pre-requested referral.
    -- Only reliable from 2025-04-03; earlier months will show 0% (feature absent).
    round(
        countIf(referral_class IN ('patient_requested_only', 'both'))
        / count(*) * 100,
        1
    )                                                                   AS patient_requested_rate_pct,

    -- Combined rate (ANY referral signal) — matches the April dashboard definition.
    -- Breaks the Feb/Mar → Apr comparability; surfaced here for audit purposes only.
    round(
        countIf(referral_class != 'no_referral')
        / count(*) * 100,
        1
    )                                                                   AS combined_any_referral_rate_pct,

    -- Flag months that contain the mixed definition so BI tools can warn users
    if(
        consult_month >= toDate('2025-04-01'),
        true,
        false
    )                                                                   AS contains_patient_intake_signal

FROM {{ ref('int_referrals_classified') }}
GROUP BY consult_month
ORDER BY consult_month

-- ─────────────────────────────────────────────────────────────────────────────
-- METRIC DEFINITION NOTE (required by assignment)
-- ─────────────────────────────────────────────────────────────────────────────
-- The 'both' case (doctor issued + patient requested) is counted in BOTH the
-- doctor_referral_rate_pct and patient_requested_rate_pct columns because both
-- independent signals fired and deserve to be surfaced in their respective
-- metrics; suppressing it from either column would undercount a genuine event.
-- It is counted only once in combined_any_referral_rate_pct to avoid
-- double-inflation of a total.
--
-- Patient-requested referrals do NOT belong in the primary clinical metric
-- (doctor_referral_rate_pct). A patient ticking a checkbox before the consult
-- is a stated preference, not a clinical assessment. Including it conflates
-- patient demand with clinician judgment, making the metric unsuitable for
-- clinical governance reporting (e.g. Ministry of Health dashboards) where it
-- is used to evaluate care quality and referral appropriateness. It may be
-- valuable as a separate operational or patient-experience metric but should
-- never be added to the clinical rate without an explicit governance decision.
-- The April dashboard error — silently merging both sources — is exactly the
-- failure mode this separation is designed to prevent.
