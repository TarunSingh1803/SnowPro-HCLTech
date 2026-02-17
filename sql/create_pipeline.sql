USE DATABASE ABC;
USE SCHEMA POLICY_SCHEMA;

-- ============================================================
-- AUTOMATED PIPELINE: Snowpipe + Streams + Tasks
-- RAW (auto-ingest) → SILVER (clean) → GOLD (normalize)
-- ============================================================


-- ============================================================
-- STEP 1: SNOWPIPES — Auto-ingest files into RAW tables
-- ============================================================

-- Snowpipe for Policies (CSV)
CREATE OR REPLACE PIPE POLICY_PIPE
  AUTO_INGEST = FALSE          -- set TRUE + configure cloud notifications for full automation
AS
COPY INTO RAW_POLICIES
FROM @POLICY_STAGES
FILE_FORMAT = STREAM_CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Snowpipe for Claims (NDJSON)
CREATE OR REPLACE PIPE CLAIMS_PIPE
  AUTO_INGEST = FALSE
AS
COPY INTO RAW_CLAIMS (
    claim_id, policy_number, customer_id, state, city, zip,
    claim_type, incident_date, fnol_datetime, status,
    report_channel, total_incurred, total_paid
)
FROM (
    SELECT
        $1:claim_id::VARCHAR,
        $1:policy_number::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:address.state::VARCHAR,
        $1:address.city::VARCHAR,
        $1:address.zip::VARCHAR,
        $1:claim_type::VARCHAR,
        $1:incident_date::VARCHAR,
        $1:fnol_datetime::VARCHAR,
        $1:status::VARCHAR,
        $1:report_channel::VARCHAR,
        $1:total_incurred::VARCHAR,
        $1:total_paid::VARCHAR
    FROM @CLAIM_STAGES
)
FILE_FORMAT = STREAM_JSON_FORMAT
ON_ERROR = 'CONTINUE';


-- ============================================================
-- STEP 2: STREAMS — Change Data Capture on RAW tables
-- ============================================================

-- Stream on RAW_POLICIES: captures inserts/updates/deletes
CREATE OR REPLACE STREAM RAW_POLICIES_STREAM
  ON TABLE RAW_POLICIES
  APPEND_ONLY = TRUE;           -- we only care about new rows

-- Stream on RAW_CLAIMS: captures inserts
CREATE OR REPLACE STREAM RAW_CLAIMS_STREAM
  ON TABLE RAW_CLAIMS
  APPEND_ONLY = TRUE;

-- Streams on SILVER tables to feed GOLD layer
CREATE OR REPLACE STREAM SILVER_POLICIES_STREAM
  ON TABLE SILVER_POLICY_CLEANED_DATA
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM SILVER_CLAIMS_STREAM
  ON TABLE SILVER_CLAIMS_CLEANED_DATA
  APPEND_ONLY = TRUE;


-- ============================================================
-- STEP 3: TASKS — Automated transformation pipeline
-- ============================================================

-- ------------------------------------------------------------
-- TASK 1: RAW_POLICIES → SILVER_POLICY_CLEANED_DATA
-- Runs every 5 minutes WHEN the stream has new data
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_RAW_TO_SILVER_POLICIES
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW_POLICIES_STREAM')
AS
INSERT INTO SILVER_POLICY_CLEANED_DATA
SELECT
    -- Keys
    TRIM(policy_number)                                         AS policy_number,
    TRIM(customer_id)                                           AS customer_id,

    -- Name
    TRIM(first_name)                                            AS first_name,
    TRIM(last_name)                                             AS last_name,

    -- SSN: insert dashes if 9-digit no-dash format
    CASE
        WHEN TRIM(ssn) REGEXP '^[0-9]{9}$'
            THEN SUBSTR(TRIM(ssn),1,3)||'-'||SUBSTR(TRIM(ssn),4,2)||'-'||SUBSTR(TRIM(ssn),6,4)
        ELSE TRIM(ssn)
    END                                                         AS ssn,

    -- Email: trim, lowercase, fix missing @
    CASE
        WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN NULL
        WHEN LOWER(TRIM(email)) LIKE '%example.com'
             AND LOWER(TRIM(email)) NOT LIKE '%@example.com'
            THEN REPLACE(LOWER(TRIM(email)), 'example.com', '@example.com')
        ELSE LOWER(TRIM(email))
    END                                                         AS email,

    -- Phone
    NULLIF(TRIM(phone), '')                                     AS phone,

    -- Address
    TRIM(address)                                               AS address,
    TRIM(city)                                                  AS city,

    -- State: UP → Uttar Pradesh
    CASE
        WHEN UPPER(TRIM(state)) = 'UP' THEN 'Uttar Pradesh'
        ELSE TRIM(state)
    END                                                         AS state,

    -- ZIP: clean
    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN NULL
        ELSE LPAD(REPLACE(REPLACE(TRIM(zip), '-', ''), 'O', '0'), 6, '0')
    END                                                         AS zip,

    -- is_zip_valid
    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN FALSE
        WHEN REGEXP_LIKE(LPAD(REPLACE(REPLACE(TRIM(zip),'-',''),'O','0'),6,'0'), '^[0-9]{6}$') THEN TRUE
        ELSE FALSE
    END                                                         AS is_zip_valid,

    -- Policy type
    UPPER(TRIM(policy_type))                                    AS policy_type,

    -- Dates
    TRY_TO_DATE(REPLACE(TRIM(effective_date), '/', '-'))        AS effective_date,
    TRY_TO_DATE(REPLACE(TRIM(expiration_date), '/', '-'))       AS expiration_date,

    -- Premium
    TRY_TO_DOUBLE(REPLACE(TRIM(annual_premium), ',', ''))       AS annual_premium,

    -- is_premium_valid
    CASE
        WHEN annual_premium IS NULL OR TRIM(annual_premium) = '' THEN FALSE
        WHEN TRY_TO_DOUBLE(REPLACE(TRIM(annual_premium), ',', '')) < 0 THEN FALSE
        ELSE TRUE
    END                                                         AS is_premium_valid,

    -- Payment frequency, renewal, status, marital
    UPPER(TRIM(payment_frequency))                              AS payment_frequency,
    UPPER(TRIM(renewal_flag))                                   AS renewal_flag,
    UPPER(TRIM(policy_status))                                  AS policy_status,
    INITCAP(TRIM(marital_status))                               AS marital_status,

    -- Agent
    TRIM(agent_id)                                              AS agent_id,
    TRIM(agency_name)                                           AS agency_name,
    TRIM(agency_region)                                         AS agency_region,

    -- is_email_valid
    CASE
        WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN FALSE
        WHEN LOWER(TRIM(email)) LIKE '%example.com'
             AND LOWER(TRIM(email)) NOT LIKE '%@example.com' THEN TRUE
        WHEN REGEXP_LIKE(LOWER(TRIM(email)), '.*@.*\\..*') THEN TRUE
        ELSE FALSE
    END                                                         AS is_email_valid,

    -- is_ssn_valid
    CASE
        WHEN TRIM(ssn) IS NULL OR TRIM(ssn) = '' THEN FALSE
        WHEN TRIM(ssn) REGEXP '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' THEN TRUE
        WHEN TRIM(ssn) REGEXP '^[0-9]{9}$' THEN TRUE
        ELSE FALSE
    END                                                         AS is_ssn_valid,

    -- is_phone_valid
    CASE
        WHEN TRIM(phone) IS NULL OR TRIM(phone) = '' THEN FALSE
        ELSE TRUE
    END                                                         AS is_phone_valid,

    CURRENT_TIMESTAMP()                                         AS cleaned_at

FROM RAW_POLICIES_STREAM;


-- ------------------------------------------------------------
-- TASK 2: RAW_CLAIMS → SILVER_CLAIMS_CLEANED_DATA
-- Child task: runs after TASK_RAW_TO_SILVER_POLICIES
-- Also has its own stream check
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_RAW_TO_SILVER_CLAIMS
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW_CLAIMS_STREAM')
AS
INSERT INTO SILVER_CLAIMS_CLEANED_DATA
SELECT
    TRIM(claim_id)                                              AS claim_id,
    NULLIF(TRIM(policy_number), '')                             AS policy_number,
    NULLIF(TRIM(customer_id), '')                               AS customer_id,

    CASE
        WHEN UPPER(TRIM(state)) = 'UP' THEN 'Uttar Pradesh'
        ELSE TRIM(state)
    END                                                         AS state,
    TRIM(city)                                                  AS city,

    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN NULL
        ELSE LPAD(REPLACE(REPLACE(TRIM(zip), '-', ''), 'O', '0'), 6, '0')
    END                                                         AS zip,

    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN FALSE
        WHEN REGEXP_LIKE(LPAD(REPLACE(REPLACE(TRIM(zip),'-',''),'O','0'),6,'0'), '^[0-9]{6}$') THEN TRUE
        ELSE FALSE
    END                                                         AS is_zip_valid,

    UPPER(TRIM(claim_type))                                     AS claim_type,
    TRY_TO_DATE(REPLACE(TRIM(incident_date), '/', '-'))         AS incident_date,
    TRY_TO_TIMESTAMP_NTZ(TRIM(fnol_datetime))                   AS fnol_datetime,
    UPPER(TRIM(status))                                         AS status,
    LOWER(TRIM(report_channel))                                 AS report_channel,

    TRY_TO_DOUBLE(REPLACE(TRIM(total_incurred), ',', ''))       AS total_incurred,
    ABS(TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')))      AS total_paid,

    CASE WHEN policy_number IS NULL OR TRIM(policy_number) = '' THEN FALSE ELSE TRUE END AS has_policy_link,
    CASE WHEN TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')) < 0 THEN TRUE ELSE FALSE END AS is_paid_negative,
    CASE
        WHEN total_incurred IS NOT NULL AND TRIM(total_incurred) != ''
         AND total_paid IS NOT NULL AND TRIM(total_paid) != ''
         AND TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')) >
             TRY_TO_DOUBLE(REPLACE(TRIM(total_incurred), ',', ''))
        THEN TRUE ELSE FALSE
    END                                                         AS is_overpaid,

    FALSE                                                       AS is_duplicate,
    CURRENT_TIMESTAMP()                                         AS cleaned_at

FROM RAW_CLAIMS_STREAM;


-- ------------------------------------------------------------
-- TASK 3: SILVER → GOLD_CUSTOMERS (MERGE for upsert)
-- Runs every 5 minutes when silver policies stream has data
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_SILVER_TO_GOLD_CUSTOMERS
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER_POLICIES_STREAM')
AS
MERGE INTO GOLD_CUSTOMERS tgt
USING (
    SELECT customer_id, first_name, last_name, ssn, marital_status, email, phone
    FROM SILVER_POLICIES_STREAM
    WHERE customer_id IS NOT NULL AND customer_id != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cleaned_at DESC) = 1
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    first_name     = src.first_name,
    last_name      = src.last_name,
    ssn            = src.ssn,
    marital_status = src.marital_status,
    email          = src.email,
    phone          = src.phone,
    updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (customer_id, first_name, last_name, ssn, marital_status, email, phone)
    VALUES (src.customer_id, src.first_name, src.last_name, src.ssn, src.marital_status, src.email, src.phone);


-- ------------------------------------------------------------
-- TASK 4: SILVER → GOLD_ADDRESSES (MERGE)
-- AFTER TASK_SILVER_TO_GOLD_CUSTOMERS (FK dependency)
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_SILVER_TO_GOLD_ADDRESSES
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_SILVER_TO_GOLD_CUSTOMERS
AS
MERGE INTO GOLD_ADDRESSES tgt
USING (
    SELECT customer_id, address AS street_address, city, state, zip
    FROM SILVER_POLICY_CLEANED_DATA
    WHERE customer_id IS NOT NULL AND customer_id != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cleaned_at DESC) = 1
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    street_address = src.street_address,
    city           = src.city,
    state          = src.state,
    zip            = src.zip,
    updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (customer_id, street_address, city, state, zip)
    VALUES (src.customer_id, src.street_address, src.city, src.state, src.zip);


-- ------------------------------------------------------------
-- TASK 5: SILVER → GOLD_AGENTS (MERGE)
-- AFTER TASK_SILVER_TO_GOLD_CUSTOMERS
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_SILVER_TO_GOLD_AGENTS
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_SILVER_TO_GOLD_CUSTOMERS
AS
MERGE INTO GOLD_AGENTS tgt
USING (
    SELECT agent_id, agency_name, agency_region
    FROM SILVER_POLICY_CLEANED_DATA
    WHERE agent_id IS NOT NULL AND agent_id != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY cleaned_at DESC) = 1
) src
ON tgt.agent_id = src.agent_id
WHEN MATCHED THEN UPDATE SET
    agency_name   = src.agency_name,
    agency_region = src.agency_region,
    updated_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (agent_id, agency_name, agency_region)
    VALUES (src.agent_id, src.agency_name, src.agency_region);


-- ------------------------------------------------------------
-- TASK 6: SILVER → GOLD_POLICIES (MERGE)
-- AFTER TASK_SILVER_TO_GOLD_AGENTS (needs both customers + agents)
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_SILVER_TO_GOLD_POLICIES
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_SILVER_TO_GOLD_AGENTS
AS
MERGE INTO GOLD_POLICIES tgt
USING (
    SELECT policy_number, customer_id, agent_id, policy_type,
           effective_date, expiration_date, annual_premium,
           policy_status, renewal_flag, payment_frequency
    FROM SILVER_POLICY_CLEANED_DATA
    WHERE policy_number IS NOT NULL AND policy_number != ''
      AND customer_id IS NOT NULL AND customer_id != ''
      AND agent_id IS NOT NULL AND agent_id != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_number ORDER BY cleaned_at DESC) = 1
) src
ON tgt.policy_number = src.policy_number
WHEN MATCHED THEN UPDATE SET
    customer_id       = src.customer_id,
    agent_id          = src.agent_id,
    policy_type       = src.policy_type,
    effective_date    = src.effective_date,
    expiration_date   = src.expiration_date,
    annual_premium    = src.annual_premium,
    policy_status     = src.policy_status,
    renewal_flag      = src.renewal_flag,
    payment_frequency = src.payment_frequency,
    updated_at        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (policy_number, customer_id, agent_id, policy_type, effective_date, expiration_date, annual_premium, policy_status, renewal_flag, payment_frequency)
    VALUES (src.policy_number, src.customer_id, src.agent_id, src.policy_type, src.effective_date, src.expiration_date, src.annual_premium, src.policy_status, src.renewal_flag, src.payment_frequency);


-- ------------------------------------------------------------
-- TASK 7: SILVER → GOLD_CLAIMS (MERGE)
-- Independent schedule (claims stream)
-- ------------------------------------------------------------

CREATE OR REPLACE TASK TASK_SILVER_TO_GOLD_CLAIMS
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER_CLAIMS_STREAM')
AS
MERGE INTO GOLD_CLAIMS tgt
USING (
    SELECT claim_id, policy_number, customer_id, claim_type,
           incident_date, fnol_datetime, status, report_channel,
           total_incurred, total_paid
    FROM SILVER_CLAIMS_STREAM
    WHERE claim_id IS NOT NULL AND claim_id != ''
      AND is_duplicate = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY cleaned_at DESC) = 1
) src
ON tgt.claim_id = src.claim_id
WHEN MATCHED THEN UPDATE SET
    policy_number  = src.policy_number,
    customer_id    = src.customer_id,
    claim_type     = src.claim_type,
    incident_date  = src.incident_date,
    fnol_datetime  = src.fnol_datetime,
    status         = src.status,
    report_channel = src.report_channel,
    total_incurred = src.total_incurred,
    total_paid     = src.total_paid,
    updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (claim_id, policy_number, customer_id, claim_type, incident_date, fnol_datetime, status, report_channel, total_incurred, total_paid)
    VALUES (src.claim_id, src.policy_number, src.customer_id, src.claim_type, src.incident_date, src.fnol_datetime, src.status, src.report_channel, src.total_incurred, src.total_paid);


-- ============================================================
-- STEP 4: RESUME ALL TASKS (tasks are created in SUSPENDED state)
-- ============================================================

-- Resume child tasks FIRST (bottom-up)
ALTER TASK TASK_SILVER_TO_GOLD_POLICIES RESUME;
ALTER TASK TASK_SILVER_TO_GOLD_AGENTS RESUME;
ALTER TASK TASK_SILVER_TO_GOLD_ADDRESSES RESUME;

-- Then resume root tasks
ALTER TASK TASK_SILVER_TO_GOLD_CUSTOMERS RESUME;
ALTER TASK TASK_SILVER_TO_GOLD_CLAIMS RESUME;
ALTER TASK TASK_RAW_TO_SILVER_POLICIES RESUME;
ALTER TASK TASK_RAW_TO_SILVER_CLAIMS RESUME;


-- ============================================================
-- STEP 5: VERIFY — Check all pipeline objects
-- ============================================================

-- List pipes
SHOW PIPES IN SCHEMA POLICY_SCHEMA;

-- List streams
SHOW STREAMS IN SCHEMA POLICY_SCHEMA;

-- List tasks
SHOW TASKS IN SCHEMA POLICY_SCHEMA;
