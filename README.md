# Irembo TeleClinic — Analytics Engineer Take-Home Assignment

## Source Schema Assumptions

1. **`consultations` raw table** contains a `status` column with values `completed`, `cancelled`, `no_show`. Completed consultations are the denominator for all clinical rate metrics.
2. **`consultation_started_at`** is stored as a string (or cast-to-string DateTime) in the raw layer, allowing `positionCaseInsensitive` to detect the `+02:00` / `+0200` offset patterns introduced by app v2.0 (released 2025-04-05).
3. **`intake_flags`** is accessible via `stg_consultation_requests` (existing staging model). If it is a separate raw table, the `{{ ref('stg_consultation_requests') }}` call in `int_referrals_classified.sql` should be replaced with `{{ source('teleclinic_raw', 'intake_flags') }}` — this is noted inline in the model.
4. **`referral_requested`** is `NULL` for all consultations before 2025-04-03 (the intake checkbox did not exist). The model coalesces `NULL → false` so pre-April rows are correctly classified as `no_referral` rather than dropped.
5. **`patient_id`** values beginning with `TEST_` are seed/QA accounts. All other `patient_id` formats (numeric, UUID, alphanumeric without that prefix) are treated as real patients.
6. **Wait-time implausibility threshold** is set at 480 minutes (8 hours). TeleClinic is a same-day virtual service; any queue time longer than a full working day is assumed to be a data error (e.g. a consultation created one day and started the following day due to a system glitch).
7. **ClickHouse dialect**: `lagInFrame()` is used in the singular anomaly-detection test because ClickHouse does not support standard SQL `LAG()` without the `ROWS BETWEEN` frame clause in all versions. Adjust to `neighbor()` if running on an older ClickHouse version that lacks window function support.
8. **`dbt_utils`** package is assumed to be installed (used for `expression_is_true` generic test in schema_tests.yml). Add `dbt-utils` to `packages.yml` if not already present.

## AI Tool Usage

- **Claude (Anthropic)** was used to review the assignment brief, stress-test the metric definition reasoning (particularly the 'both' referral classification), and proof-read the written responses for clarity and completeness.
- All SQL logic, test thresholds, and diagnostic conclusions are my own. The AI was used as a sounding board, not as a code generator.
- Every SQL statement was written and reviewed by hand with explicit attention to ClickHouse dialect compatibility.
