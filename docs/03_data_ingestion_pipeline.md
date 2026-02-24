# ABC Insurance — Data Ingestion & Pipeline

> **Project:** SnowPro Hackathon — HCL Tech  
> **Version:** 1.0  
> **Date:** 2026-02-17

---

## 1. Pipeline Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          PIPELINE ORCHESTRATION                            │
│                                                                            │
│  ┌─────────┐   PUT    ┌─────────┐  COPY INTO  ┌─────────┐               │
│  │  FILES   │────────▶│  STAGE  │────────────▶│   RAW   │               │
│  │ CSV/JSON │         └─────────┘              └────┬────┘               │
│  └─────────┘                                       │                      │
│                                            STREAM (CDC)                   │
│                                                    │                      │
│                                            ┌───────▼───────┐             │
│                                            │  TASK_RAW_TO  │             │
│                                            │  _VALIDATED   │             │
│                                            │  (MERGE +     │             │
│                                            │   JS UDFs)    │             │
│                                            └───────┬───────┘             │
│                                                    │                      │
│                                            ┌───────▼───────┐             │
│                                            │  TASK_VAL_TO  │             │
│                                            │  _CURATED     │             │
│                                            │  (MERGE +     │             │
│                                            │   Enrichment) │             │
│                                            └───────┬───────┘             │
│                                                    │                      │
│                                            ┌───────▼───────┐             │
│                                            │   ANALYTICS   │             │
│                                            │   VIEWS       │             │
│                                            │  (REQ1–REQ8)  │             │
│                                            └───────────────┘             │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. File Upload to Stages

```sql
-- Policy files (CSV)
PUT file:///path/to/policies_master_v3.csv @RAW.raw_policy_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

PUT file:///path/to/policies_inc_2026-02-15_v3.csv @RAW.raw_policy_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- Claims files (NDJSON)
PUT file:///path/to/claims_master_v3.json @RAW.raw_claims_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

PUT file:///path/to/claims_inc_2026-02-15_v3.json @RAW.raw_claims_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- Verify
LIST @RAW.raw_policy_stage;
LIST @RAW.raw_claims_stage;
```

---

## 3. COPY INTO (RAW Layer Load)

```sql
-- Load policies (CSV → RAW)
COPY INTO RAW.raw_policies (
    policy_number, customer_id, first_name, last_name, ssn,
    email, phone, address, city, state, zip,
    policy_type, effective_date, expiration_date, annual_premium,
    payment_frequency, renewal_flag, policy_status, marital_status,
    agent_id, agency_name, agency_region
)
FROM @RAW.raw_policy_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_policy_format')
ON_ERROR = 'CONTINUE'
PURGE = FALSE
FORCE = FALSE;

-- Load claims (NDJSON → RAW)
COPY INTO RAW.raw_claims (raw_data)
FROM @RAW.raw_claims_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.json_claims_format')
ON_ERROR = 'CONTINUE'
PURGE = FALSE
FORCE = FALSE;

-- Validate load
SELECT 'raw_policies' AS table_name, COUNT(*) AS row_count FROM RAW.raw_policies
UNION ALL
SELECT 'raw_claims',                 COUNT(*)              FROM RAW.raw_claims;

-- Check COPY history for errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'raw_policies',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'Loaded';

-- Audit log
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_policies', COUNT(*), 'SUCCESS' FROM RAW.raw_policies;

INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_claims', COUNT(*), 'SUCCESS' FROM RAW.raw_claims;
```

---

## 4. JavaScript UDFs

```sql
USE SCHEMA VALIDATED;

-- Email Validation
CREATE OR REPLACE FUNCTION udf_validate_email(email VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!EMAIL) return false;
    var pattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    return pattern.test(EMAIL);
$$;

-- Phone Validation
CREATE OR REPLACE FUNCTION udf_validate_phone(phone VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!PHONE) return false;
    var digits = PHONE.replace(/\D/g, '');
    return digits.length === 10 || (digits.length === 11 && digits[0] === '1');
$$;

-- ZIP Code Validation
CREATE OR REPLACE FUNCTION udf_validate_zip(zip VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!ZIP) return false;
    var pattern = /^\d{5}(-\d{4})?$/;
    return pattern.test(ZIP.trim());
$$;

-- SSN Validation
CREATE OR REPLACE FUNCTION udf_validate_ssn(ssn VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!SSN) return false;
    var pattern = /^\d{3}-\d{2}-\d{4}$/;
    return pattern.test(SSN.trim());
$$;

-- SSN Masking
CREATE OR REPLACE FUNCTION udf_mask_ssn(ssn VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    if (!SSN) return null;
    return 'XXX-XX-' + SSN.trim().slice(-4);
$$;
```

---

## 5. Streams (CDC)

```sql
-- Streams on RAW tables
USE SCHEMA RAW;

CREATE OR REPLACE STREAM stream_raw_policies
  ON TABLE RAW.raw_policies
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw policy data';

CREATE OR REPLACE STREAM stream_raw_claims
  ON TABLE RAW.raw_claims
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw claims data';

-- Streams on VALIDATED tables
USE SCHEMA VALIDATED;

CREATE OR REPLACE STREAM stream_validated_policies
  ON TABLE VALIDATED.validated_policies
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated policy data';

CREATE OR REPLACE STREAM stream_validated_claims
  ON TABLE VALIDATED.validated_claims
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated claims data';
```

---

## 6. MERGE: RAW → VALIDATED

### 6.1 Policies

```sql
MERGE INTO VALIDATED.validated_policies AS tgt
USING (
    SELECT
        policy_number,
        customer_id,
        INITCAP(TRIM(first_name))                          AS first_name,
        INITCAP(TRIM(last_name))                           AS last_name,
        TRIM(ssn)                                          AS ssn,
        LOWER(TRIM(email))                                 AS email,
        TRIM(phone)                                        AS phone,
        TRIM(address)                                      AS address,
        INITCAP(TRIM(city))                                AS city,
        UPPER(TRIM(state))                                 AS state,
        TRIM(zip)                                          AS zip,
        UPPER(TRIM(policy_type))                           AS policy_type,
        TRY_TO_DATE(effective_date, 'YYYY-MM-DD')          AS effective_date,
        TRY_TO_DATE(expiration_date, 'YYYY-MM-DD')         AS expiration_date,
        TRY_TO_NUMBER(annual_premium, 12, 2)               AS annual_premium,
        UPPER(TRIM(payment_frequency))                     AS payment_frequency,
        CASE UPPER(TRIM(renewal_flag))
            WHEN 'Y' THEN TRUE WHEN 'YES' THEN TRUE
            WHEN 'N' THEN FALSE WHEN 'NO' THEN FALSE
            ELSE NULL
        END                                                AS renewal_flag,
        UPPER(TRIM(policy_status))                         AS policy_status,
        INITCAP(TRIM(marital_status))                      AS marital_status,
        TRIM(agent_id)                                     AS agent_id,
        TRIM(agency_name)                                  AS agency_name,
        TRIM(agency_region)                                AS agency_region,

        VALIDATED.udf_validate_email(email)                AS is_valid_email,
        VALIDATED.udf_validate_phone(phone)                AS is_valid_phone,
        VALIDATED.udf_validate_zip(zip)                    AS is_valid_zip,
        VALIDATED.udf_validate_ssn(ssn)                    AS is_valid_ssn,
        (TRY_TO_DATE(expiration_date, 'YYYY-MM-DD')
            > TRY_TO_DATE(effective_date, 'YYYY-MM-DD'))   AS is_valid_dates,
        (TRY_TO_NUMBER(annual_premium, 12, 2) > 0)        AS is_valid_premium,

        _loaded_at,
        _source_file,
        _row_number

    FROM RAW.stream_raw_policies

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY policy_number
        ORDER BY _loaded_at DESC, _row_number DESC
    ) = 1
) AS src
ON tgt.policy_number = src.policy_number

WHEN MATCHED THEN UPDATE SET
    tgt.customer_id         = src.customer_id,
    tgt.first_name          = src.first_name,
    tgt.last_name           = src.last_name,
    tgt.ssn                 = src.ssn,
    tgt.email               = src.email,
    tgt.phone               = src.phone,
    tgt.address             = src.address,
    tgt.city                = src.city,
    tgt.state               = src.state,
    tgt.zip                 = src.zip,
    tgt.policy_type         = src.policy_type,
    tgt.effective_date      = src.effective_date,
    tgt.expiration_date     = src.expiration_date,
    tgt.annual_premium      = src.annual_premium,
    tgt.payment_frequency   = src.payment_frequency,
    tgt.renewal_flag        = src.renewal_flag,
    tgt.policy_status       = src.policy_status,
    tgt.marital_status      = src.marital_status,
    tgt.agent_id            = src.agent_id,
    tgt.agency_name         = src.agency_name,
    tgt.agency_region       = src.agency_region,
    tgt.is_valid_email      = src.is_valid_email,
    tgt.is_valid_phone      = src.is_valid_phone,
    tgt.is_valid_zip        = src.is_valid_zip,
    tgt.is_valid_ssn        = src.is_valid_ssn,
    tgt.is_valid_dates      = src.is_valid_dates,
    tgt.is_valid_premium    = src.is_valid_premium,
    tgt.is_valid_record     = (src.is_valid_email AND src.is_valid_phone
                               AND src.is_valid_zip AND src.is_valid_ssn
                               AND src.is_valid_dates AND src.is_valid_premium),
    tgt.validation_errors   = ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
                                IFF(NOT src.is_valid_email,   'INVALID_EMAIL',   NULL),
                                IFF(NOT src.is_valid_phone,   'INVALID_PHONE',   NULL),
                                IFF(NOT src.is_valid_zip,     'INVALID_ZIP',     NULL),
                                IFF(NOT src.is_valid_ssn,     'INVALID_SSN',     NULL),
                                IFF(NOT src.is_valid_dates,   'INVALID_DATES',   NULL),
                                IFF(NOT src.is_valid_premium, 'INVALID_PREMIUM', NULL)
                              ), '|'),
    tgt._validated_at       = CURRENT_TIMESTAMP(),
    tgt._source_file        = src._source_file

WHEN NOT MATCHED THEN INSERT (
    policy_number, customer_id, first_name, last_name, ssn,
    email, phone, address, city, state, zip,
    policy_type, effective_date, expiration_date, annual_premium,
    payment_frequency, renewal_flag, policy_status, marital_status,
    agent_id, agency_name, agency_region,
    is_valid_email, is_valid_phone, is_valid_zip, is_valid_ssn,
    is_valid_dates, is_valid_premium, is_valid_record, validation_errors,
    _loaded_at, _validated_at, _source_file, _row_number
) VALUES (
    src.policy_number, src.customer_id, src.first_name, src.last_name, src.ssn,
    src.email, src.phone, src.address, src.city, src.state, src.zip,
    src.policy_type, src.effective_date, src.expiration_date, src.annual_premium,
    src.payment_frequency, src.renewal_flag, src.policy_status, src.marital_status,
    src.agent_id, src.agency_name, src.agency_region,
    src.is_valid_email, src.is_valid_phone, src.is_valid_zip, src.is_valid_ssn,
    src.is_valid_dates, src.is_valid_premium,
    (src.is_valid_email AND src.is_valid_phone AND src.is_valid_zip
     AND src.is_valid_ssn AND src.is_valid_dates AND src.is_valid_premium),
    ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
        IFF(NOT src.is_valid_email,   'INVALID_EMAIL',   NULL),
        IFF(NOT src.is_valid_phone,   'INVALID_PHONE',   NULL),
        IFF(NOT src.is_valid_zip,     'INVALID_ZIP',     NULL),
        IFF(NOT src.is_valid_ssn,     'INVALID_SSN',     NULL),
        IFF(NOT src.is_valid_dates,   'INVALID_DATES',   NULL),
        IFF(NOT src.is_valid_premium, 'INVALID_PREMIUM', NULL)
    ), '|'),
    src._loaded_at, CURRENT_TIMESTAMP(), src._source_file, src._row_number
);
```

### 6.2 Claims

```sql
MERGE INTO VALIDATED.validated_claims AS tgt
USING (
    SELECT
        raw_data:claim_id::VARCHAR(50)                              AS claim_id,
        raw_data:policy_number::VARCHAR(50)                         AS policy_number,
        raw_data:customer_id::VARCHAR(50)                           AS customer_id,
        UPPER(raw_data:structured_address.state::VARCHAR(10))       AS state,
        INITCAP(raw_data:structured_address.city::VARCHAR(100))     AS city,
        raw_data:structured_address.zip::VARCHAR(20)                AS zip,
        UPPER(raw_data:claim_type::VARCHAR(50))                     AS claim_type,
        TRY_TO_DATE(raw_data:incident_date::VARCHAR)                AS incident_date,
        TRY_TO_TIMESTAMP_NTZ(raw_data:fnol_datetime::VARCHAR)       AS fnol_datetime,
        UPPER(raw_data:status::VARCHAR(30))                         AS status,
        UPPER(raw_data:report_channel::VARCHAR(50))                 AS report_channel,
        TRY_TO_NUMBER(raw_data:total_incurred::VARCHAR, 14, 2)      AS total_incurred,
        TRY_TO_NUMBER(raw_data:total_paid::VARCHAR, 14, 2)          AS total_paid,

        _loaded_at,
        _source_file,
        _row_number

    FROM RAW.stream_raw_claims

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY raw_data:claim_id::VARCHAR
        ORDER BY _loaded_at DESC, _row_number DESC
    ) = 1
) AS src
ON tgt.claim_id = src.claim_id

WHEN MATCHED THEN UPDATE SET
    tgt.policy_number     = src.policy_number,
    tgt.customer_id       = src.customer_id,
    tgt.state             = src.state,
    tgt.city              = src.city,
    tgt.zip               = src.zip,
    tgt.claim_type        = src.claim_type,
    tgt.incident_date     = src.incident_date,
    tgt.fnol_datetime     = src.fnol_datetime,
    tgt.status            = src.status,
    tgt.report_channel    = src.report_channel,
    tgt.total_incurred    = src.total_incurred,
    tgt.total_paid        = src.total_paid,
    tgt.is_valid_dates    = (src.incident_date <= src.fnol_datetime::DATE),
    tgt.is_valid_amounts  = (src.total_paid <= src.total_incurred
                             AND src.total_incurred >= 0
                             AND src.total_paid >= 0),
    tgt.has_matching_policy = EXISTS (
        SELECT 1 FROM VALIDATED.validated_policies vp
        WHERE vp.policy_number = src.policy_number
    ),
    tgt.is_valid_record   = (
        (src.incident_date <= src.fnol_datetime::DATE)
        AND (src.total_paid <= src.total_incurred AND src.total_incurred >= 0 AND src.total_paid >= 0)
    ),
    tgt._validated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file      = src._source_file

WHEN NOT MATCHED THEN INSERT (
    claim_id, policy_number, customer_id,
    state, city, zip,
    claim_type, incident_date, fnol_datetime,
    status, report_channel, total_incurred, total_paid,
    is_valid_dates, is_valid_amounts, has_matching_policy, is_valid_record,
    validation_errors,
    _loaded_at, _validated_at, _source_file, _row_number
) VALUES (
    src.claim_id, src.policy_number, src.customer_id,
    src.state, src.city, src.zip,
    src.claim_type, src.incident_date, src.fnol_datetime,
    src.status, src.report_channel, src.total_incurred, src.total_paid,
    (src.incident_date <= src.fnol_datetime::DATE),
    (src.total_paid <= src.total_incurred AND src.total_incurred >= 0 AND src.total_paid >= 0),
    EXISTS (SELECT 1 FROM VALIDATED.validated_policies vp WHERE vp.policy_number = src.policy_number),
    (src.incident_date <= src.fnol_datetime::DATE)
        AND (src.total_paid <= src.total_incurred AND src.total_incurred >= 0 AND src.total_paid >= 0),
    ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
        IFF(NOT (src.incident_date <= src.fnol_datetime::DATE), 'INVALID_DATES', NULL),
        IFF(NOT (src.total_paid <= src.total_incurred), 'PAID_EXCEEDS_INCURRED', NULL),
        IFF(src.total_incurred < 0, 'NEGATIVE_INCURRED', NULL),
        IFF(src.total_paid < 0, 'NEGATIVE_PAID', NULL)
    ), '|'),
    src._loaded_at, CURRENT_TIMESTAMP(), src._source_file, src._row_number
);
```

---

## 7. MERGE: VALIDATED → CURATED (Normalization)

### 7.1 → customers

```sql
MERGE INTO CURATED.customers AS tgt
USING (
    SELECT
        customer_id,
        first_name,
        last_name,
        ssn,
        marital_status,
        email,
        phone,
        _source_file

    FROM VALIDATED.validated_policies
    WHERE is_valid_record = TRUE

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _validated_at DESC, effective_date DESC
    ) = 1
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
    tgt.first_name      = src.first_name,
    tgt.last_name       = src.last_name,
    tgt.ssn             = src.ssn,
    tgt.marital_status  = src.marital_status,
    tgt.email           = src.email,
    tgt.phone           = src.phone,
    tgt._updated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file    = src._source_file

WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, ssn, marital_status,
    email, phone,
    _created_at, _updated_at, _source_file
) VALUES (
    src.customer_id, src.first_name, src.last_name, src.ssn, src.marital_status,
    src.email, src.phone,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);
```

### 7.2 → addresses

```sql
MERGE INTO CURATED.addresses AS tgt
USING (
    SELECT
        customer_id,
        TRIM(address)                   AS street_address,
        INITCAP(TRIM(city))             AS city,
        UPPER(TRIM(state))              AS state,
        TRIM(zip)                       AS zip,
        _source_file

    FROM VALIDATED.validated_policies
    WHERE is_valid_record = TRUE
      AND address IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _validated_at DESC, effective_date DESC
    ) = 1
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
    tgt.street_address  = src.street_address,
    tgt.city            = src.city,
    tgt.state           = src.state,
    tgt.zip             = src.zip,
    tgt._updated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file    = src._source_file

WHEN NOT MATCHED THEN INSERT (
    customer_id, street_address, city, state, zip,
    _created_at, _updated_at, _source_file
) VALUES (
    src.customer_id, src.street_address, src.city, src.state, src.zip,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);
```

### 7.3 → agents

```sql
MERGE INTO CURATED.agents AS tgt
USING (
    SELECT
        agent_id,
        agency_name,
        agency_region,
        _source_file

    FROM VALIDATED.validated_policies
    WHERE is_valid_record = TRUE
      AND agent_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY agent_id
        ORDER BY _validated_at DESC
    ) = 1
) AS src
ON tgt.agent_id = src.agent_id

WHEN MATCHED THEN UPDATE SET
    tgt.agency_name     = src.agency_name,
    tgt.agency_region   = src.agency_region,
    tgt._updated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file    = src._source_file

WHEN NOT MATCHED THEN INSERT (
    agent_id, agency_name, agency_region,
    _created_at, _updated_at, _source_file
) VALUES (
    src.agent_id, src.agency_name, src.agency_region,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);
```

### 7.4 → policies

```sql
MERGE INTO CURATED.policies AS tgt
USING (
    SELECT
        vp.policy_number,
        vp.customer_id,
        vp.agent_id,
        vp.policy_type,
        vp.effective_date,
        vp.expiration_date,
        vp.annual_premium,
        vp.payment_frequency,
        vp.renewal_flag,
        vp.policy_status,

        DATEDIFF('day', vp.effective_date, vp.expiration_date)  AS policy_term_days,
        ROUND(vp.annual_premium / 12, 2)                        AS premium_per_month,
        (UPPER(vp.policy_status) = 'ACTIVE')                    AS is_active,

        vp._source_file

    FROM VALIDATED.validated_policies vp
    WHERE vp.is_valid_record = TRUE

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY vp.policy_number
        ORDER BY vp._validated_at DESC
    ) = 1
) AS src
ON tgt.policy_number = src.policy_number

WHEN MATCHED THEN UPDATE SET
    tgt.customer_id       = src.customer_id,
    tgt.agent_id          = src.agent_id,
    tgt.policy_type       = src.policy_type,
    tgt.effective_date    = src.effective_date,
    tgt.expiration_date   = src.expiration_date,
    tgt.annual_premium    = src.annual_premium,
    tgt.payment_frequency = src.payment_frequency,
    tgt.renewal_flag      = src.renewal_flag,
    tgt.policy_status     = src.policy_status,
    tgt.policy_term_days  = src.policy_term_days,
    tgt.premium_per_month = src.premium_per_month,
    tgt.is_active         = src.is_active,
    tgt._updated_at       = CURRENT_TIMESTAMP(),
    tgt._source_file      = src._source_file

WHEN NOT MATCHED THEN INSERT (
    policy_number, customer_id, agent_id,
    policy_type, effective_date, expiration_date, annual_premium,
    payment_frequency, renewal_flag, policy_status,
    policy_term_days, premium_per_month, is_active,
    _created_at, _updated_at, _source_file
) VALUES (
    src.policy_number, src.customer_id, src.agent_id,
    src.policy_type, src.effective_date, src.expiration_date, src.annual_premium,
    src.payment_frequency, src.renewal_flag, src.policy_status,
    src.policy_term_days, src.premium_per_month, src.is_active,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);
```

### 7.5 → claims

```sql
MERGE INTO CURATED.claims AS tgt
USING (
    SELECT
        claim_id, policy_number, customer_id,
        claim_type, incident_date, fnol_datetime,
        status, report_channel, total_incurred, total_paid,

        ROUND(DATEDIFF('hour', incident_date, fnol_datetime), 2) AS claim_response_time_hrs,
        ROUND(total_paid / NULLIF(total_incurred, 0), 4)         AS paid_to_incurred_ratio,
        (UPPER(status) = 'CLOSED')                               AS is_closed,
        CASE
            WHEN total_incurred < 5000   THEN 'LOW'
            WHEN total_incurred < 20000  THEN 'MEDIUM'
            WHEN total_incurred < 50000  THEN 'HIGH'
            ELSE 'CRITICAL'
        END                                                      AS severity_bucket,

        _source_file

    FROM VALIDATED.validated_claims
    WHERE is_valid_record = TRUE

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY claim_id
        ORDER BY _validated_at DESC
    ) = 1
) AS src
ON tgt.claim_id = src.claim_id

WHEN MATCHED THEN UPDATE SET
    tgt.policy_number          = src.policy_number,
    tgt.customer_id            = src.customer_id,
    tgt.claim_type             = src.claim_type,
    tgt.incident_date          = src.incident_date,
    tgt.fnol_datetime          = src.fnol_datetime,
    tgt.status                 = src.status,
    tgt.report_channel         = src.report_channel,
    tgt.total_incurred         = src.total_incurred,
    tgt.total_paid             = src.total_paid,
    tgt.claim_response_time_hrs = src.claim_response_time_hrs,
    tgt.paid_to_incurred_ratio = src.paid_to_incurred_ratio,
    tgt.is_closed              = src.is_closed,
    tgt.severity_bucket        = src.severity_bucket,
    tgt._updated_at            = CURRENT_TIMESTAMP(),
    tgt._source_file           = src._source_file

WHEN NOT MATCHED THEN INSERT (
    claim_id, policy_number, customer_id,
    claim_type, incident_date, fnol_datetime,
    status, report_channel, total_incurred, total_paid,
    claim_response_time_hrs, paid_to_incurred_ratio, is_closed, severity_bucket,
    _created_at, _updated_at, _source_file
) VALUES (
    src.claim_id, src.policy_number, src.customer_id,
    src.claim_type, src.incident_date, src.fnol_datetime,
    src.status, src.report_channel, src.total_incurred, src.total_paid,
    src.claim_response_time_hrs, src.paid_to_incurred_ratio, src.is_closed, src.severity_bucket,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);
```

### 7.6 Validate Normalization

```sql
-- Row counts
SELECT 'customers'  AS tbl, COUNT(*) AS rows FROM CURATED.customers
UNION ALL
SELECT 'addresses',          COUNT(*)        FROM CURATED.addresses
UNION ALL
SELECT 'agents',             COUNT(*)        FROM CURATED.agents
UNION ALL
SELECT 'policies',           COUNT(*)        FROM CURATED.policies
UNION ALL
SELECT 'claims',             COUNT(*)        FROM CURATED.claims;

-- FK integrity checks
SELECT 'orphan_policies (no customer)' AS check_name, COUNT(*) AS count
FROM CURATED.policies p
WHERE NOT EXISTS (SELECT 1 FROM CURATED.customers c WHERE c.customer_id = p.customer_id)
UNION ALL
SELECT 'orphan_policies (no agent)', COUNT(*)
FROM CURATED.policies p
WHERE p.agent_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM CURATED.agents a WHERE a.agent_id = p.agent_id)
UNION ALL
SELECT 'orphan_addresses (no customer)', COUNT(*)
FROM CURATED.addresses ad
WHERE NOT EXISTS (SELECT 1 FROM CURATED.customers c WHERE c.customer_id = ad.customer_id)
UNION ALL
SELECT 'orphan_claims (no policy)', COUNT(*)
FROM CURATED.claims cl
WHERE NOT EXISTS (SELECT 1 FROM CURATED.policies p WHERE p.policy_number = cl.policy_number)
UNION ALL
SELECT 'orphan_claims (no customer)', COUNT(*)
FROM CURATED.claims cl
WHERE cl.customer_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM CURATED.customers c WHERE c.customer_id = cl.customer_id);
```

---

## 8. Tasks (Automation)

```sql
-- Warehouse
CREATE OR REPLACE WAREHOUSE task_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Dedicated warehouse for pipeline tasks';

-- Task: RAW → VALIDATED (Policies)
CREATE OR REPLACE TASK task_raw_to_validated_policies
  WAREHOUSE = task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_policies')
AS
  -- (MERGE from Section 6.1)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_validated_policies completed');

-- Task: RAW → VALIDATED (Claims)
CREATE OR REPLACE TASK task_raw_to_validated_claims
  WAREHOUSE = task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_claims')
AS
  -- (MERGE from Section 6.2)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_validated_claims completed');

-- Task: VALIDATED → CURATED Entities (customers + addresses + agents)
CREATE OR REPLACE TASK task_curate_entities
  WAREHOUSE = task_wh
  AFTER task_raw_to_validated_policies
AS
BEGIN
  -- Step A: customers (Section 7.1)
  -- Step B: addresses (Section 7.2)
  -- Step C: agents (Section 7.3)
  CALL SYSTEM$LOG('INFO', 'task_curate_entities completed');
END;

-- Task: VALIDATED → CURATED Policies
CREATE OR REPLACE TASK task_curate_policies
  WAREHOUSE = task_wh
  AFTER task_curate_entities
AS
  -- (MERGE from Section 7.4)
  CALL SYSTEM$LOG('INFO', 'task_curate_policies completed');

-- Task: VALIDATED → CURATED Claims
CREATE OR REPLACE TASK task_curate_claims
  WAREHOUSE = task_wh
  AFTER task_raw_to_validated_claims, task_curate_entities
AS
  -- (MERGE from Section 7.5)
  CALL SYSTEM$LOG('INFO', 'task_curate_claims completed');

-- Resume all tasks (reverse dependency order)
ALTER TASK task_curate_claims            RESUME;
ALTER TASK task_curate_policies          RESUME;
ALTER TASK task_curate_entities          RESUME;
ALTER TASK task_raw_to_validated_policies RESUME;
ALTER TASK task_raw_to_validated_claims   RESUME;

-- Monitor tasks
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
));

SELECT SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_policies');
SELECT SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_claims');
```

---

## 9. Task DAG

```
task_raw_to_validated_policies ──┬──▶ task_curate_entities ──┬──▶ task_curate_policies
       (root, scheduled)        │  (child: customers,       │     (child)
                                │   addresses, agents)      │
                                │                           │
task_raw_to_validated_claims ───┘───────────────────────────┴──▶ task_curate_claims
       (root, scheduled)                                         (child)
```

---

*End of Document*
