USE DATABASE ABC;
USE SCHEMA POLICY_SCHEMA;

-- ============================================
-- GOLD LAYER: NORMALIZED TABLES
-- ============================================

/* ============================================================
   1. GOLD_CUSTOMERS TABLE
   ============================================================ */

CREATE OR REPLACE TABLE GOLD_CUSTOMERS (
    customer_id        STRING        NOT NULL,
    first_name         STRING,
    last_name          STRING,
    ssn                STRING,
    marital_status     STRING,
    email              STRING,
    phone              STRING,

    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at         TIMESTAMP,

    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

INSERT INTO GOLD_CUSTOMERS (customer_id, first_name, last_name, ssn, marital_status, email, phone)
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    ssn,
    marital_status,
    email,
    phone
FROM SILVER_POLICY_CLEANED_DATA
WHERE customer_id IS NOT NULL AND customer_id != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cleaned_at DESC) = 1;


/* ============================================================
   2. GOLD_ADDRESSES TABLE (1:1 with Customers)
   ============================================================ */

CREATE OR REPLACE TABLE GOLD_ADDRESSES (
    customer_id        STRING        NOT NULL,
    street_address     STRING,
    city               STRING,
    state              STRING,
    zip                STRING,

    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at         TIMESTAMP,

    CONSTRAINT pk_addresses PRIMARY KEY (customer_id),

    CONSTRAINT fk_addresses_customer
        FOREIGN KEY (customer_id)
        REFERENCES GOLD_CUSTOMERS(customer_id)
);

INSERT INTO GOLD_ADDRESSES (customer_id, street_address, city, state, zip)
SELECT DISTINCT
    customer_id,
    address,
    city,
    state,
    zip
FROM SILVER_POLICY_CLEANED_DATA
WHERE customer_id IS NOT NULL AND customer_id != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cleaned_at DESC) = 1;


/* ============================================================
   3. GOLD_AGENTS TABLE
   ============================================================ */

CREATE OR REPLACE TABLE GOLD_AGENTS (
    agent_id        STRING        NOT NULL,
    agency_name     STRING,
    agency_region   STRING,

    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP,

    CONSTRAINT pk_agents PRIMARY KEY (agent_id)
);

INSERT INTO GOLD_AGENTS (agent_id, agency_name, agency_region)
SELECT DISTINCT
    agent_id,
    agency_name,
    agency_region
FROM SILVER_POLICY_CLEANED_DATA
WHERE agent_id IS NOT NULL AND agent_id != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY cleaned_at DESC) = 1;


/* ============================================================
   4. GOLD_POLICIES TABLE
   ============================================================ */

CREATE OR REPLACE TABLE GOLD_POLICIES (
    policy_number     STRING        NOT NULL,
    customer_id       STRING        NOT NULL,
    agent_id          STRING        NOT NULL,
    policy_type       STRING,
    effective_date    DATE,
    expiration_date   DATE,
    annual_premium    NUMBER(12,2),
    policy_status     STRING,
    renewal_flag      STRING,
    payment_frequency STRING,

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at        TIMESTAMP,

    CONSTRAINT pk_policies PRIMARY KEY (policy_number),

    CONSTRAINT fk_policies_customer
        FOREIGN KEY (customer_id)
        REFERENCES GOLD_CUSTOMERS(customer_id),

    CONSTRAINT fk_policies_agent
        FOREIGN KEY (agent_id)
        REFERENCES GOLD_AGENTS(agent_id)
);

INSERT INTO GOLD_POLICIES (policy_number, customer_id, agent_id, policy_type, effective_date, expiration_date, annual_premium, policy_status, renewal_flag, payment_frequency)
SELECT
    policy_number,
    customer_id,
    agent_id,
    policy_type,
    effective_date,
    expiration_date,
    annual_premium,
    policy_status,
    renewal_flag,
    payment_frequency
FROM SILVER_POLICY_CLEANED_DATA
WHERE policy_number IS NOT NULL AND policy_number != ''
  AND customer_id IS NOT NULL AND customer_id != ''
  AND agent_id IS NOT NULL AND agent_id != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_number ORDER BY cleaned_at DESC) = 1;


/* ============================================================
   5. GOLD_CLAIMS TABLE
   ============================================================ */

CREATE OR REPLACE TABLE GOLD_CLAIMS (
    claim_id         STRING        NOT NULL,
    policy_number    STRING,
    customer_id      STRING,
    claim_type       STRING,
    incident_date    DATE,
    fnol_datetime    TIMESTAMP,
    status           STRING,
    report_channel   STRING,
    total_incurred   NUMBER(12,2),
    total_paid       NUMBER(12,2),

    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP,

    CONSTRAINT pk_claims PRIMARY KEY (claim_id)
);

INSERT INTO GOLD_CLAIMS (claim_id, policy_number, customer_id, claim_type, incident_date, fnol_datetime, status, report_channel, total_incurred, total_paid)
SELECT
    claim_id,
    policy_number,
    customer_id,
    claim_type,
    incident_date,
    fnol_datetime,
    status,
    report_channel,
    total_incurred,
    total_paid
FROM SILVER_CLAIMS_CLEANED_DATA
WHERE claim_id IS NOT NULL AND claim_id != ''
  AND is_duplicate = FALSE
QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY cleaned_at DESC) = 1;
