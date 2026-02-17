USE DATABASE ABC;
USE SCHEMA POLICY_SCHEMA;

-- ============================================
-- SILVER LAYER: CLEANED POLICY DATA
-- ============================================

CREATE OR REPLACE TABLE SILVER_POLICY_CLEANED_DATA AS
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

    -- Email: trim whitespace, lowercase, fix missing @ before example.com
    CASE
        WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN NULL
        WHEN LOWER(TRIM(email)) LIKE '%example.com'
             AND LOWER(TRIM(email)) NOT LIKE '%@example.com'
            THEN REPLACE(LOWER(TRIM(email)), 'example.com', '@example.com')
        ELSE LOWER(TRIM(email))
    END                                                         AS email,

    -- Phone: trim
    NULLIF(TRIM(phone), '')                                     AS phone,

    -- Address
    TRIM(address)                                               AS address,
    TRIM(city)                                                  AS city,

    -- State: normalize UP -> Uttar Pradesh
    CASE
        WHEN UPPER(TRIM(state)) = 'UP' THEN 'Uttar Pradesh'
        ELSE TRIM(state)
    END                                                         AS state,

    -- ZIP: remove hyphens, replace O with 0, pad to 6 digits
    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN NULL
        ELSE LPAD(REPLACE(REPLACE(TRIM(zip), '-', ''), 'O', '0'), 6, '0')
    END                                                         AS zip,

    -- ZIP valid flag
    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN FALSE
        WHEN REGEXP_LIKE(LPAD(REPLACE(REPLACE(TRIM(zip),'-',''),'O','0'),6,'0'), '^[0-9]{6}$') THEN TRUE
        ELSE FALSE
    END                                                         AS is_zip_valid,

    -- Policy type: uppercase
    UPPER(TRIM(policy_type))                                    AS policy_type,

    -- Dates: standardize to DATE (replace / with -)
    TRY_TO_DATE(REPLACE(TRIM(effective_date), '/', '-'))        AS effective_date,
    TRY_TO_DATE(REPLACE(TRIM(expiration_date), '/', '-'))       AS expiration_date,

    -- Premium: remove commas, cast to number
    TRY_TO_DOUBLE(REPLACE(TRIM(annual_premium), ',', ''))       AS annual_premium,

    -- Premium valid flag
    CASE
        WHEN annual_premium IS NULL OR TRIM(annual_premium) = '' THEN FALSE
        WHEN TRY_TO_DOUBLE(REPLACE(TRIM(annual_premium), ',', '')) < 0 THEN FALSE
        ELSE TRUE
    END                                                         AS is_premium_valid,

    -- Payment frequency: uppercase
    UPPER(TRIM(payment_frequency))                              AS payment_frequency,

    -- Renewal flag: uppercase Y/N
    UPPER(TRIM(renewal_flag))                                   AS renewal_flag,

    -- Policy status: uppercase
    UPPER(TRIM(policy_status))                                  AS policy_status,

    -- Marital status: proper case
    INITCAP(TRIM(marital_status))                               AS marital_status,

    -- Agent
    TRIM(agent_id)                                              AS agent_id,
    TRIM(agency_name)                                           AS agency_name,
    TRIM(agency_region)                                         AS agency_region,

    -- Data quality flags
    CASE
        WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN FALSE
        WHEN LOWER(TRIM(email)) LIKE '%example.com'
             AND LOWER(TRIM(email)) NOT LIKE '%@example.com' THEN TRUE
        WHEN REGEXP_LIKE(LOWER(TRIM(email)), '.*@.*\\..*') THEN TRUE
        ELSE FALSE
    END                                                         AS is_email_valid,

    CASE
        WHEN TRIM(ssn) IS NULL OR TRIM(ssn) = '' THEN FALSE
        WHEN TRIM(ssn) REGEXP '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' THEN TRUE
        WHEN TRIM(ssn) REGEXP '^[0-9]{9}$' THEN TRUE
        ELSE FALSE
    END                                                         AS is_ssn_valid,

    CASE
        WHEN TRIM(phone) IS NULL OR TRIM(phone) = '' THEN FALSE
        ELSE TRUE
    END                                                         AS is_phone_valid,

    CURRENT_TIMESTAMP()                                         AS cleaned_at

FROM RAW_POLICIES;


-- ============================================
-- SILVER LAYER: CLEANED CLAIMS DATA
-- ============================================

CREATE OR REPLACE TABLE SILVER_CLAIMS_CLEANED_DATA AS
SELECT
    -- Keys
    TRIM(claim_id)                                              AS claim_id,
    NULLIF(TRIM(policy_number), '')                             AS policy_number,
    NULLIF(TRIM(customer_id), '')                               AS customer_id,

    -- Address
    CASE
        WHEN UPPER(TRIM(state)) = 'UP' THEN 'Uttar Pradesh'
        ELSE TRIM(state)
    END                                                         AS state,
    TRIM(city)                                                  AS city,

    -- ZIP: remove hyphens, replace O with 0, pad to 6 digits
    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN NULL
        ELSE LPAD(REPLACE(REPLACE(TRIM(zip), '-', ''), 'O', '0'), 6, '0')
    END                                                         AS zip,

    CASE
        WHEN TRIM(zip) IS NULL OR TRIM(zip) = '' THEN FALSE
        WHEN REGEXP_LIKE(LPAD(REPLACE(REPLACE(TRIM(zip),'-',''),'O','0'),6,'0'), '^[0-9]{6}$') THEN TRUE
        ELSE FALSE
    END                                                         AS is_zip_valid,

    -- Claim type: uppercase
    UPPER(TRIM(claim_type))                                     AS claim_type,

    -- Dates: standardize
    TRY_TO_DATE(REPLACE(TRIM(incident_date), '/', '-'))         AS incident_date,
    TRY_TO_TIMESTAMP_NTZ(TRIM(fnol_datetime))                   AS fnol_datetime,

    -- Status: uppercase
    UPPER(TRIM(status))                                         AS status,

    -- Report channel: lowercase
    LOWER(TRIM(report_channel))                                 AS report_channel,

    -- Amounts: remove commas, cast (ABS for total_paid)
    TRY_TO_DOUBLE(REPLACE(TRIM(total_incurred), ',', ''))       AS total_incurred,
    ABS(TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')))      AS total_paid,

    -- Data quality flags
    CASE
        WHEN policy_number IS NULL OR TRIM(policy_number) = '' THEN FALSE
        ELSE TRUE
    END                                                         AS has_policy_link,

    CASE
        WHEN TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')) < 0 THEN TRUE
        ELSE FALSE
    END                                                         AS is_paid_negative,

    CASE
        WHEN total_incurred IS NOT NULL AND TRIM(total_incurred) != ''
         AND total_paid IS NOT NULL AND TRIM(total_paid) != ''
         AND TRY_TO_DOUBLE(REPLACE(TRIM(total_paid), ',', '')) >
             TRY_TO_DOUBLE(REPLACE(TRIM(total_incurred), ',', ''))
        THEN TRUE
        ELSE FALSE
    END                                                         AS is_overpaid,

    -- Duplicate flag (will update after)
    FALSE                                                       AS is_duplicate,

    CURRENT_TIMESTAMP()                                         AS cleaned_at

FROM RAW_CLAIMS;

-- Mark duplicates (keep first occurrence, flag rest)
UPDATE SILVER_CLAIMS_CLEANED_DATA t
SET is_duplicate = TRUE
WHERE t.cleaned_at != (
    SELECT MIN(s.cleaned_at)
    FROM SILVER_CLAIMS_CLEANED_DATA s
    WHERE s.claim_id = t.claim_id
)
AND t.claim_id IN (
    SELECT claim_id FROM SILVER_CLAIMS_CLEANED_DATA GROUP BY claim_id HAVING COUNT(*) > 1
);
