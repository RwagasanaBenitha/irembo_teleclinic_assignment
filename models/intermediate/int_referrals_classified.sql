-- int_referrals_classified.sql
-- Intermediate model: one row per completed consultation, classifying the
-- referral status based on both clinical and patient-intake signals.
--
-- Classification logic:
--   both                  → doctor issued a referral AND patient had pre-ticked the checkbox
--   doctor_referral       → doctor issued a referral; patient had NOT pre-ticked
--   patient_requested_only→ doctor did NOT issue a referral; patient had pre-ticked
--   no_referral           → neither signal present
--
-- The 'both' case is important: it means the doctor's clinical decision aligned
-- with the patient's prior request. We surface it as a distinct class so that
-- downstream mart models can choose how to count it without losing information.

WITH completed_consults AS (
    SELECT
        consultation_id,
        patient_id,
        provider_id,
        consultation_created_at,
        consultation_started_at_utc,
        wait_time_minutes,
        is_tz_corrected,
        channel,
        app_version,
        -- Truncate to month for partitioning in the mart
        toStartOfMonth(consultation_created_at) AS consult_month
    FROM {{ ref('stg_consultations_fixed') }}
    WHERE status = 'completed'
),

clinical AS (
    SELECT
        consultation_id,
        -- Coerce NULL to false so joins don't silently drop rows
        coalesce(referral_issued, false) AS doctor_referral_issued
    FROM {{ ref('stg_clinical_outcomes') }}
),

intake AS (
    SELECT
        consultation_id,
        -- referral_requested is NULL for all records before 2025-04-03 (feature not yet live).
        -- Treat NULL as false; downstream tests confirm no unexpected NULLs post-launch.
        coalesce(referral_requested, false) AS patient_referral_requested
    FROM {{ ref('stg_consultation_requests') }}
    -- NOTE: If intake_flags is a separate source table (not stg_consultation_requests),
    -- replace the ref above with: {{ source('teleclinic_raw', 'intake_flags') }}
    -- Assumption documented in README.
),

classified AS (
    SELECT
        c.consultation_id,
        c.patient_id,
        c.provider_id,
        c.consult_month,
        c.consultation_created_at,
        c.consultation_started_at_utc,
        c.wait_time_minutes,
        c.is_tz_corrected,
        c.channel,
        c.app_version,

        cl.doctor_referral_issued,
        i.patient_referral_requested,

        -- Primary classification column used by the mart
        CASE
            WHEN cl.doctor_referral_issued AND i.patient_referral_requested THEN 'both'
            WHEN cl.doctor_referral_issued AND NOT i.patient_referral_requested THEN 'doctor_referral'
            WHEN NOT cl.doctor_referral_issued AND i.patient_referral_requested THEN 'patient_requested_only'
            ELSE 'no_referral'
        END AS referral_class

    FROM completed_consults AS c
    -- LEFT JOINs: a completed consult may legitimately have no clinical outcome row
    -- (e.g. incomplete data load); we preserve it and treat missing as false above.
    LEFT JOIN clinical AS cl USING (consultation_id)
    LEFT JOIN intake   AS i  USING (consultation_id)
)

SELECT *
FROM classified
