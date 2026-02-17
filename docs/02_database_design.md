# ABC Insurance — Database Design (DDL)

> **Project:** SnowPro Hackathon — HCL Tech  
> **Version:** 1.0  
> **Date:** 2026-02-17

---

## 1. Database & Schemas

```sql
CREATE DATABASE IF NOT EXISTS ABC_INSURANCE_DB;
USE DATABASE ABC_INSURANCE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS VALIDATED;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS SECURITY;
CREATE SCHEMA IF NOT EXISTS AUDIT;
```

---

## 2. File Formats

```sql
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT csv_policy_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null', 'N/A')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

CREATE OR REPLACE FILE FORMAT json_claims_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  STRIP_NULL_VALUES = FALSE
  IGNORE_UTF8_ERRORS = TRUE;
```

---

## 3. Internal Stages

```sql
USE SCHEMA RAW;

CREATE OR REPLACE STAGE raw_policy_stage
  FILE_FORMAT = csv_policy_format
  COMMENT = 'Stage for policy CSV files';

CREATE OR REPLACE STAGE raw_claims_stage
  FILE_FORMAT = json_claims_format
  COMMENT = 'Stage for claims NDJSON files';
```

---

## 4. RAW Layer Tables

### 4.1 raw_policies

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_policies (
    policy_number       VARCHAR(50),
    customer_id         VARCHAR(50),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    ssn                 VARCHAR(20),
    email               VARCHAR(255),
    phone               VARCHAR(30),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(10),
    zip                 VARCHAR(20),
    policy_type         VARCHAR(50),
    effective_date      VARCHAR(20),
    expiration_date     VARCHAR(20),
    annual_premium      VARCHAR(30),
    payment_frequency   VARCHAR(30),
    renewal_flag        VARCHAR(10),
    policy_status       VARCHAR(30),
    marital_status      VARCHAR(30),
    agent_id            VARCHAR(50),
    agency_name         VARCHAR(200),
    agency_region       VARCHAR(100),

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500) DEFAULT METADATA$FILENAME,
    _row_number         NUMBER       DEFAULT METADATA$FILE_ROW_NUMBER
);
```

### 4.2 raw_claims

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_claims (
    raw_data            VARIANT,

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500) DEFAULT METADATA$FILENAME,
    _row_number         NUMBER       DEFAULT METADATA$FILE_ROW_NUMBER
);
```

---

## 5. VALIDATED Layer Tables

### 5.1 validated_policies

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE validated_policies (
    policy_number           VARCHAR(50)     NOT NULL,
    customer_id             VARCHAR(50)     NOT NULL,
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    ssn                     VARCHAR(20),
    email                   VARCHAR(255),
    phone                   VARCHAR(30),
    address                 VARCHAR(500),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    zip                     VARCHAR(20),
    policy_type             VARCHAR(50),
    effective_date          DATE,
    expiration_date         DATE,
    annual_premium          NUMBER(12,2),
    payment_frequency       VARCHAR(30),
    renewal_flag            BOOLEAN,
    policy_status           VARCHAR(30),
    marital_status          VARCHAR(30),
    agent_id                VARCHAR(50),
    agency_name             VARCHAR(200),
    agency_region           VARCHAR(100),

    is_valid_email          BOOLEAN,
    is_valid_phone          BOOLEAN,
    is_valid_zip            BOOLEAN,
    is_valid_ssn            BOOLEAN,
    is_valid_dates          BOOLEAN,
    is_valid_premium        BOOLEAN,
    is_valid_record         BOOLEAN,
    validation_errors       VARCHAR(2000),

    _loaded_at              TIMESTAMP_NTZ,
    _validated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),
    _row_number             NUMBER,

    CONSTRAINT pk_validated_policies PRIMARY KEY (policy_number)
);
```

### 5.2 validated_claims

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE validated_claims (
    claim_id                VARCHAR(50)     NOT NULL,
    policy_number           VARCHAR(50)     NOT NULL,
    customer_id             VARCHAR(50),
    state                   VARCHAR(10),
    city                    VARCHAR(100),
    zip                     VARCHAR(20),
    claim_type              VARCHAR(50),
    incident_date           DATE,
    fnol_datetime           TIMESTAMP_NTZ,
    status                  VARCHAR(30),
    report_channel          VARCHAR(50),
    total_incurred          NUMBER(14,2),
    total_paid              NUMBER(14,2),

    is_valid_dates          BOOLEAN,
    is_valid_amounts        BOOLEAN,
    has_matching_policy     BOOLEAN,
    is_valid_record         BOOLEAN,
    validation_errors       VARCHAR(2000),

    _loaded_at              TIMESTAMP_NTZ,
    _validated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),
    _row_number             NUMBER,

    CONSTRAINT pk_validated_claims PRIMARY KEY (claim_id)
);
```

---

## 6. CURATED Layer Tables (Normalized — 5 Tables)

### 6.1 customers

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE customers (
    customer_id             VARCHAR(50)     NOT NULL,
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    ssn                     VARCHAR(20),
    marital_status          VARCHAR(30),
    email                   VARCHAR(255),
    phone                   VARCHAR(30),

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);
```

### 6.2 addresses

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE addresses (
    customer_id             VARCHAR(50)     NOT NULL,
    street_address          VARCHAR(500),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    zip                     VARCHAR(20),

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_addresses PRIMARY KEY (customer_id)
);
```

### 6.3 agents

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE agents (
    agent_id                VARCHAR(50)     NOT NULL,
    agency_name             VARCHAR(200),
    agency_region           VARCHAR(100),

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_agents PRIMARY KEY (agent_id)
);
```

### 6.4 policies

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE policies (
    policy_number           VARCHAR(50)     NOT NULL,
    customer_id             VARCHAR(50)     NOT NULL,
    agent_id                VARCHAR(50),
    policy_type             VARCHAR(50),
    effective_date          DATE,
    expiration_date         DATE,
    annual_premium          NUMBER(12,2),
    payment_frequency       VARCHAR(30),
    renewal_flag            BOOLEAN,
    policy_status           VARCHAR(30),

    policy_term_days        NUMBER,
    premium_per_month       NUMBER(12,2),
    is_active               BOOLEAN,

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_policies PRIMARY KEY (policy_number)
);
```

### 6.5 claims

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE claims (
    claim_id                VARCHAR(50)     NOT NULL,
    policy_number           VARCHAR(50)     NOT NULL,
    customer_id             VARCHAR(50),
    claim_type              VARCHAR(50),
    incident_date           DATE,
    fnol_datetime           TIMESTAMP_NTZ,
    status                  VARCHAR(30),
    report_channel          VARCHAR(50),
    total_incurred          NUMBER(14,2),
    total_paid              NUMBER(14,2),

    claim_response_time_hrs NUMBER(10,2),
    paid_to_incurred_ratio  NUMBER(6,4),
    is_closed               BOOLEAN,
    severity_bucket         VARCHAR(20),

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_claims PRIMARY KEY (claim_id)
);
```

### 6.6 Convenience Views

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE VIEW v_policies_full AS
SELECT
    p.policy_number,
    p.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name  AS customer_full_name,
    c.ssn,
    c.email,
    c.phone,
    c.marital_status,
    a.street_address                    AS address,
    a.city,
    a.state,
    a.zip,
    p.policy_type,
    p.effective_date,
    p.expiration_date,
    p.annual_premium,
    p.payment_frequency,
    p.renewal_flag,
    p.policy_status,
    p.policy_term_days,
    p.premium_per_month,
    p.is_active,
    p.agent_id,
    ag.agency_name,
    ag.agency_region
FROM policies p
LEFT JOIN customers c  ON p.customer_id = c.customer_id
LEFT JOIN agents ag     ON p.agent_id    = ag.agent_id
LEFT JOIN addresses a   ON p.customer_id = a.customer_id;

CREATE OR REPLACE VIEW v_claims_full AS
SELECT
    cl.claim_id,
    cl.policy_number,
    cl.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name  AS customer_full_name,
    cl.claim_type,
    cl.incident_date,
    cl.fnol_datetime,
    cl.status,
    cl.report_channel,
    cl.total_incurred,
    cl.total_paid,
    cl.claim_response_time_hrs,
    cl.paid_to_incurred_ratio,
    cl.is_closed,
    cl.severity_bucket,
    p.policy_type,
    p.policy_status,
    ag.agency_name,
    ag.agency_region
FROM claims cl
LEFT JOIN customers c   ON cl.customer_id  = c.customer_id
LEFT JOIN policies p     ON cl.policy_number = p.policy_number
LEFT JOIN agents ag      ON p.agent_id       = ag.agent_id;
```

---

## 7. SECURITY Schema

```sql
USE SCHEMA SECURITY;

CREATE OR REPLACE TABLE region_role_mapping (
    role_name       VARCHAR(100)    NOT NULL,
    allowed_region  VARCHAR(100)    NOT NULL,
    _created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO region_role_mapping (role_name, allowed_region) VALUES
    ('ADMIN_ROLE',          'ALL'),
    ('ANALYST_ROLE',        'ALL'),
    ('REGION_AGENT_EAST',   'East'),
    ('REGION_AGENT_WEST',   'West'),
    ('REGION_AGENT_NORTH',  'North'),
    ('REGION_AGENT_SOUTH',  'South');

CREATE ROLE IF NOT EXISTS ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS REGION_AGENT_ROLE;

CREATE USER IF NOT EXISTS demo_admin     PASSWORD = 'Admin@123'     DEFAULT_ROLE = ADMIN_ROLE;
CREATE USER IF NOT EXISTS demo_analyst   PASSWORD = 'Analyst@123'   DEFAULT_ROLE = ANALYST_ROLE;
CREATE USER IF NOT EXISTS demo_agent     PASSWORD = 'Agent@123'     DEFAULT_ROLE = REGION_AGENT_ROLE;

GRANT ROLE ADMIN_ROLE         TO USER demo_admin;
GRANT ROLE ANALYST_ROLE       TO USER demo_analyst;
GRANT ROLE REGION_AGENT_ROLE  TO USER demo_agent;
```

---

## 8. AUDIT Schema

```sql
USE SCHEMA AUDIT;

CREATE OR REPLACE TABLE audit_log (
    log_id          NUMBER AUTOINCREMENT,
    operation       VARCHAR(50),
    target_table    VARCHAR(200),
    rows_affected   NUMBER,
    status          VARCHAR(20),
    error_message   VARCHAR(4000),
    executed_by     VARCHAR(100)  DEFAULT CURRENT_USER(),
    executed_role   VARCHAR(100)  DEFAULT CURRENT_ROLE(),
    executed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 9. ER Diagram

```
┌───────────────────┐
│    customers      │
├───────────────────┤
│ customer_id PK    │──────────────────────────────────────────────┐
│ first_name        │                                              │
│ last_name         │                                   ┌──────────┴────────┐
│ ssn               │                                   │     addresses     │
│ marital_status    │                                   ├───────────────────┤
│ email             │                                   │ customer_id PK/FK │
│ phone             │                                   │ street_address    │
└────────┬──────────┘                                   │ city              │
         │ 1:N                                          │ state             │
         │                                              │ zip               │
         │                                              └───────────────────┘
         │ 1:N
┌────────▼──────────┐    1:N     ┌───────────────────┐
│     policies      │───────────▶│      claims       │
├───────────────────┤            ├───────────────────┤
│ policy_number PK  │            │ claim_id PK       │
│ customer_id FK    │            │ policy_number FK  │
│ agent_id FK       │            │ customer_id FK    │
│ policy_type       │            │ claim_type        │
│ effective_date    │            │ incident_date     │
│ expiration_date   │            │ fnol_datetime     │
│ annual_premium    │            │ status            │
│ policy_status     │            │ report_channel    │
│ (derived cols)    │            │ total_incurred    │
└────────┬──────────┘            │ total_paid        │
         │ N:1                   │ (derived cols)    │
         │                       └───────────────────┘
┌────────▼──────────┐
│      agents       │
├───────────────────┤
│ agent_id PK       │
│ agency_name       │
│ agency_region     │
└───────────────────┘
```

---

*End of Document*
