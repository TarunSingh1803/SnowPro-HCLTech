# ABC Insurance — Solution Architecture Document

> **Project:** SnowPro Hackathon — HCL Tech  
> **Version:** 2.0  
> **Date:** 2026-02-17  
> **Author:** Team SnowPro

---

## 1. Problem Statement

ABC Insurance needs a **data engineering and analytics platform** built entirely on Snowflake to:

- Ingest policy data (CSV, 500 + 50 incremental rows) and claims data (NDJSON, 500 + 50 incremental lines)
- Apply data quality checks — validate emails, phones, ZIP codes, SSNs, dates, and premium amounts
- Normalize denormalized source data into a clean relational schema
- Compute 8 business KPIs (REQ1–REQ8)
- Implement enterprise-grade security — RBAC, column-level masking, row-level filtering
- Handle incremental loads automatically using CDC

All of this must be accomplished using **Snowflake-native capabilities only** — no external tools, no ETL frameworks, no orchestrators.

---

## 2. Solution Approach

We implement a **4-layer architecture** inside a single Snowflake database (`ABC_INSURANCE_DB`), with each layer serving a distinct purpose:

| Layer | Schema | Purpose | Key Objects |
|-------|--------|---------|-------------|
| **Landing** | `RAW` | Ingest source files exactly as received, zero transformation | Internal stages, file formats, COPY INTO, raw tables |
| **Cleansing** | `VALIDATED` | Type-cast, validate, deduplicate, flag errors | JavaScript UDFs, MERGE, validation flags, error codes |
| **Business** | `CURATED` | Normalize into 5 relational tables, compute derived fields | customers, addresses, agents, policies, claims + views |
| **Analytics** | `ANALYTICS` | Deliver the 8 required KPIs as SQL views | v_req1 through v_req8 |

Two additional schemas provide governance:

| Schema | Purpose |
|--------|---------|
| `SECURITY` | Role mapping table, masking policies, row access policies |
| `AUDIT` | Append-only operation log for full traceability |

---

## 3. Architecture Diagram

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                    ABC_INSURANCE_DB                                      │
  │                                                                          │
  │  ┌─────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐  │
  │  │  FILES   │────▶│    RAW     │────▶│ VALIDATED  │────▶│  CURATED   │  │
  │  │ CSV/JSON │ PUT │  Schema    │CDC  │  Schema    │MERGE│  Schema    │  │
  │  └─────────┘COPY │            │     │            │     │ 5 Tables   │  │
  │              INTO │ raw_       │     │ validated_ │     │ + 2 Views  │  │
  │                   │ policies   │     │ policies   │     │            │──┐│
  │                   │ raw_       │     │ validated_ │     │ customers  │  ││
  │                   │ claims     │     │ claims     │     │ addresses  │  ││
  │                   └─────┬──────┘     └────────────┘     │ agents     │  ││
  │                         │                               │ policies   │  ││
  │                    STREAMS                              │ claims     │  ││
  │                   (Append-Only)                         └────────────┘  ││
  │                         │                                              ││
  │                    ┌────▼────┐                          ┌────────────┐  ││
  │                    │  TASKS  │                          │ ANALYTICS  │◀─┘│
  │                    │  (DAG)  │                          │  Schema    │   │
  │                    └─────────┘                          │ REQ1–REQ8  │   │
  │                                                        └────────────┘   │
  │  ┌───────────────────────────────────────────────────────────────────┐  │
  │  │ SECURITY Schema          │  AUDIT Schema                         │  │
  │  │ • 3 Roles (RBAC)         │  • audit_log (append-only)            │  │
  │  │ • Masking Policies       │  • op type, table, rows, user, time   │  │
  │  │ • Row Access Policies    │                                       │  │
  │  │ • Region-Role Mapping    │                                       │  │
  │  └───────────────────────────────────────────────────────────────────┘  │
  └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Sources

| Source | Format | Volume | Content |
|--------|--------|--------|---------|
| `policies_master_v3.csv` | CSV (22 columns) | 500 rows | Full policy data — customer PII, agent info, address, premium, dates |
| `policies_inc_2026-02-15_v3.csv` | CSV | 50 rows | Incremental policy updates |
| `claims_master_v3.json` | NDJSON | 500 lines | Claims — nested `structured_address`, amounts, dates |
| `claims_inc_2026-02-15_v3.json` | NDJSON | 50 lines | Incremental claims |

**Key challenge:** Policy CSV is heavily denormalized — customer name, SSN, email, phone, address, agent info all repeated on every row. Claims JSON has nested objects requiring flattening.

---

## 5. Layer-by-Layer Design

### 5.1 RAW Layer — Zero-Touch Ingestion

**Goal:** Land files exactly as received. No transformation, no type casting.

- **Stages:** Two internal stages — `raw_policy_stage` (CSV) and `raw_claims_stage` (NDJSON)
- **File Formats:** `csv_policy_format` (skip header, handle NULLs) and `json_claims_format` (NDJSON parsing)
- **Tables:**
  - `raw_policies` — 22 VARCHAR columns (everything as string) + 3 metadata columns (`_loaded_at`, `_source_file`, `_row_number`)
  - `raw_claims` — single VARIANT column (`raw_data`) + 3 metadata columns
- **Loading:** `COPY INTO` with `ON_ERROR = 'CONTINUE'` (bad rows skipped, not rejected), `FORCE = FALSE` (idempotent — same file never reloads)
- **Streams:** Append-only streams on both tables capture every new row for downstream processing

**Why this matters:** Raw data is immutable. If anything goes wrong downstream, we can always reprocess from RAW without re-uploading files.

### 5.2 VALIDATED Layer — Cleansing & Quality

**Goal:** Type-cast, validate, standardize, and flag every record.

- **Type Casting:** Strings → DATE, NUMBER, BOOLEAN where needed (`TRY_TO_DATE`, `TRY_TO_NUMBER` — safe casting, no failures)
- **Standardization:** `INITCAP(names)`, `UPPER(state)`, `LOWER(email)`, `TRIM()` everywhere
- **JavaScript UDFs** (5 functions):
  - `udf_validate_email` — regex check for valid email format
  - `udf_validate_phone` — strips non-digits, checks 10/11 digit US format
  - `udf_validate_zip` — 5-digit or ZIP+4 format
  - `udf_validate_ssn` — XXX-XX-XXXX format
  - `udf_mask_ssn` — returns `XXX-XX-1234` for display
- **Validation Flags:** Every record gets boolean flags (`is_valid_email`, `is_valid_phone`, etc.) and a composite `is_valid_record`
- **Error Tracking:** Pipe-separated error codes in `validation_errors` column (e.g., `INVALID_EMAIL|INVALID_DATES`)
- **Claims Flattening:** JSON VARIANT → structured columns using `raw_data:field_name::TYPE` notation
- **Deduplication:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY _loaded_at DESC)` keeps only the latest record per key
- **Loading:** MERGE (upsert) — triggered by Tasks consuming Streams

**Why this matters:** Bad data is flagged, not deleted. Analysts can query validation errors to understand data quality. Only records with `is_valid_record = TRUE` advance to CURATED.

### 5.3 CURATED Layer — Normalized Business Model

**Goal:** Transform the flat, denormalized source into a clean 5-table relational schema.

**The normalization problem:**
The source CSV has 22 columns in one flat row — customer PII, address, agent details, and policy attributes all mashed together. If a customer has 5 policies, their SSN and name are duplicated 5 times. We fix this:

| Table | PK | Source | What It Stores |
|-------|----|---------|----|
| `customers` | `customer_id` | validated_policies | Identity (name, SSN, email, phone, marital status) — stored once per customer |
| `addresses` | `customer_id` (FK) | validated_policies | Street, city, state, ZIP — one address per customer |
| `agents` | `agent_id` | validated_policies | Agency name and region — stored once per agent |
| `policies` | `policy_number` | validated_policies | Policy details + FKs to customers and agents + derived fields (`policy_term_days`, `premium_per_month`, `is_active`) |
| `claims` | `claim_id` | validated_claims | Claim details + FKs to policies and customers + derived fields (`claim_response_time_hrs`, `severity_bucket`, `paid_to_incurred_ratio`) |

**Convenience Views:** Two views join everything back together for easy KPI querying:
- `v_policies_full` — policies + customers + agents + addresses in one flat view
- `v_claims_full` — claims + customers + policies + agents in one flat view

**Why this matters:** Normalization eliminates redundancy, ensures single-source-of-truth for customer and agent data, and demonstrates proper data modeling.

### 5.4 ANALYTICS Layer — The 8 KPIs

Each KPI is a SQL view built on the curated tables/views:

| REQ | KPI Name | What It Answers |
|-----|----------|-----------------|
| REQ1 | Agent Contact Quality | Which agents have customers with invalid emails, phones, or ZIP codes? |
| REQ2 | Policy Validation by State | Which states have the most invalid policies (bad dates, premiums)? |
| REQ3 | Policy-Claim Matching | Which claims reference non-existent policies? Unmatched records? |
| REQ4 | City Premium Benchmark | What's the avg premium per city vs overall benchmark? |
| REQ5 | Top Cities Claim Severity | Which cities have the highest total/avg claim payouts? |
| REQ6 | Cross-Sell Penetration | Which customers have multiple policy types (AUTO + HOME)? |
| REQ7 | Onboarding Attachment | Do new customers file claims faster than existing ones? |
| REQ8 | Policy-Only / Claim-Only | Which policies have never had a claim? Which claims have orphan policies? |

---

## 6. Automation — Streams, Tasks & the DAG

The entire pipeline runs automatically after files are loaded:

```
┌─────────────────┐       ┌──────────────────┐       ┌────────────────────┐
│   PUT files     │──────▶│  COPY INTO RAW   │──────▶│  STREAM detects    │
│   to stage      │       │  (idempotent)    │       │  new rows          │
└─────────────────┘       └──────────────────┘       └────────┬───────────┘
                                                              │
                                                              ▼
                                                    ┌─────────────────────┐
                                                    │ task_raw_to_        │
                                                    │ validated_policies  │──┐
                                                    │ (5 min schedule)    │  │
                                                    └─────────────────────┘  │
                                                    ┌─────────────────────┐  │
                                                    │ task_raw_to_        │  │
                                                    │ validated_claims    │──┤
                                                    │ (5 min schedule)    │  │
                                                    └─────────────────────┘  │
                                                                             │
                                                              ┌──────────────▼──┐
                                                              │ task_curate_    │
                                                              │ entities        │
                                                              │ (customers +    │
                                                              │  addresses +    │
                                                              │  agents)        │
                                                              └──┬──────────┬───┘
                                                                 │          │
                                                    ┌────────────▼┐  ┌─────▼───────────┐
                                                    │ task_curate_ │  │ task_curate_    │
                                                    │ policies     │  │ claims          │
                                                    └──────────────┘  └─────────────────┘
```

- **Root Tasks** fire every 5 minutes, but only when the stream has data (`WHEN SYSTEM$STREAM_HAS_DATA`)
- **Child Tasks** use `AFTER` to chain in dependency order
- **Entity tables first** (customers, addresses, agents), then **transactional tables** (policies, claims) — because policies needs to reference existing customers and agents
- **Every MERGE is idempotent** — rerunning produces no duplicates

---

## 7. Security Architecture

### 7.1 Three Roles

| Role | Row Access | Column Access | Use Case |
|------|-----------|---------------|----------|
| `ADMIN_ROLE` | All regions | All columns (SSN, email, phone visible) | Data engineer, DBA |
| `ANALYST_ROLE` | All regions | SSN → `XXX-XX-1234`, email/phone → `****` | Business analyst |
| `REGION_AGENT_ROLE` | Only their assigned region | SSN/email/phone masked | Regional agent |

### 7.2 Implementation

**Dynamic Data Masking (REQ-OPT-A):**
- Masking policies on `ssn`, `email`, `phone` columns
- `ADMIN_ROLE` sees real values, all other roles see masked
- Applied at the column level — works transparently on any query

**Row Access Policy (REQ-OPT-B):**
- Policy on `agency_region` column
- Uses `region_role_mapping` table: each role maps to allowed regions
- `ADMIN_ROLE` and `ANALYST_ROLE` → `ALL` regions
- `REGION_AGENT_EAST` → only rows where `agency_region = 'East'`

**Audit Trail:**
- `audit_log` table captures: operation type, target table, row count, status, user, role, timestamp
- Metadata columns (`_created_at`, `_updated_at`, `_source_file`) on every curated table

### 7.3 The Security Demo (Key Differentiator)

```sql
-- Same query, 3 different results:
USE ROLE ADMIN_ROLE;
SELECT customer_id, ssn, email, agency_region FROM CURATED.v_policies_full LIMIT 5;
-- → Full data visible: 123-45-6789, john@email.com, all regions

USE ROLE ANALYST_ROLE;
SELECT customer_id, ssn, email, agency_region FROM CURATED.v_policies_full LIMIT 5;
-- → Masked: XXX-XX-6789, ****, all regions still visible

USE ROLE REGION_AGENT_ROLE;
SELECT customer_id, ssn, email, agency_region FROM CURATED.v_policies_full LIMIT 5;
-- → Masked AND filtered: only 'East' region rows visible
```

**KPI Proof:** Run REQ1 as ADMIN and ANALYST — the aggregate numbers (counts, percentages) are identical even though underlying PII is masked. This proves masking doesn't break analytics.

---

## 8. Snowflake Features Used

| Feature | Where Used | Why |
|---------|-----------|-----|
| Internal Stages | RAW layer | Landing area for files without external storage |
| File Formats | RAW layer | Reusable CSV/JSON parsing definitions |
| COPY INTO | RAW layer | High-performance bulk loading with error handling |
| VARIANT | raw_claims | Schema-on-read for semi-structured JSON |
| JavaScript UDFs | VALIDATED layer | Complex regex validation (email, phone, SSN, ZIP) |
| Streams | RAW → VALIDATED | Change data capture — detect new rows automatically |
| Tasks | All layers | Scheduled automation with DAG dependency chains |
| MERGE | All layers | Idempotent upsert — no duplicates on re-run |
| QUALIFY | VALIDATED/CURATED | Inline deduplication without subqueries |
| Dynamic Data Masking | SECURITY | Column-level PII protection |
| Row Access Policies | SECURITY | Row-level region filtering |
| TRY_TO_* functions | VALIDATED | Safe type casting — returns NULL instead of error |
| METADATA$ columns | RAW | Automatic file/row tracking without manual input |
| Views | ANALYTICS | Logical KPI layer — no data duplication |

---

## 9. Data Flow Summary

```
 policies_master_v3.csv ─┐                                    ┌─ v_req1: Agent Contact Quality
 policies_inc_*.csv ─────┤     RAW          VALIDATED         │  v_req2: Policy Valid. by State
                         ├───▶ raw_    ───▶ validated_   ───▶ │  v_req3: Policy-Claim Matching
 claims_master_v3.json ──┤     policies      policies         │  v_req4: City Premium Benchmark
 claims_inc_*.json ──────┘     raw_     ───▶ validated_  ───▶ │  v_req5: Top Cities Severity
                               claims        claims           │  v_req6: Cross-Sell Penetration
                                                              │  v_req7: Onboarding Attachment
                                                    ▼         └─ v_req8: Policy-Only/Claim-Only
                                             ┌─────────────┐
                                             │  CURATED     │
                                             │  customers   │
                                             │  addresses   │
                                             │  agents      │
                                             │  policies    │
                                             │  claims      │
                                             └─────────────┘
```

---

## 10. What Makes This Solution Stand Out

1. **100% Snowflake-native** — no external tools, no Python scripts, no orchestrators
2. **Idempotent pipeline** — any step can be re-run without creating duplicates
3. **Incremental processing** — Streams + Tasks automatically handle new files (no manual re-runs)
4. **Proper normalization** — 5-table relational model, not flat denormalized tables
5. **Full data lineage** — every row tracks its source file, load time, and validation status
6. **Enterprise security demo** — same query, 3 roles, 3 different results (masking + row filtering)
7. **JavaScript UDFs** — real regex validation (not just `IS NOT NULL` checks)
8. **Error preservation** — bad records stay in VALIDATED with error codes, not silently dropped

---

## 11. Project Structure

```
SnowPro-HCLTech/
├── docs/
│   ├── 01_solution_architecture.md      ← This document
│   ├── 02_database_design.md            ← Full DDL (all tables, views, UDFs)
│   └── 03_data_ingestion_pipeline.md    ← COPY INTO, MERGE, Streams, Tasks
├── sql/
│   ├── 00_setup/                        ← Database, schemas, stages, file formats
│   ├── 01_raw/                          ← Raw tables, COPY INTO
│   ├── 02_validated/                    ← Validated tables, JS UDFs, MERGE
│   ├── 03_curated/                      ← Normalized tables, views, MERGE
│   ├── 04_analytics/                    ← REQ1–REQ8 views
│   ├── 05_security/                     ← Masking, row access, roles, demo
│   └── 06_automation/                   ← Streams, Tasks
├── data/                                ← Source files (not committed to git)
└── README.md
```

---

*End of Document*
