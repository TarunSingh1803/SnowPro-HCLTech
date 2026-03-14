# 📌 Snowflake Hackathon Preparation — HCL Tech

## 🎯 Goal

I am preparing for a **Snowflake Hackathon (HCL Tech selection round)** where I must:

- Build an **MVP in ~8 hours**
- Demonstrate:
  - Data ingestion from an **unknown dataset**
  - Data transformation and modeling
  - Feature engineering
  - Use of **Snowflake-native capabilities**
  - Basic Understanding of  **AI/ML using Snowpark (in-database ML)**

The **first screening** requires presenting an **architecture and execution plan before coding**.

The **actual problem statement and dataset will be provided at hackathon start**, therefore the architecture must be **flexible enough to work with any structured dataset**.

---

# 📌 Architectural Approach Chosen

We are implementing a **Snowflake-Native Medallion Architecture** designed to adapt quickly to **any incoming dataset**.

---

# 🥉 Bronze Layer (RAW / Landing Layer)

## Purpose

- Ingest data **exactly as received** (no transformations).
- Support **schema-on-read ingestion** for unknown datasets.
- Preserve raw data for **traceability and reprocessing**.

## Snowflake Objects

- Internal Stage (file landing area)
- File Format (dynamic parsing definition)
- RAW table created using **INFER_SCHEMA**
- Stream on RAW table (for incremental processing)

## Why This Matters

- Works even when **schema is unknown beforehand**
- Avoids **manual column creation**
- Enables **fast onboarding of hackathon datasets**
- Mirrors **real-world enterprise ingestion patterns**

---

# 🥈 Silver Layer (Transformation Layer)

## Purpose

Clean, standardize, and structure the ingested dataset.

## Operations

- Data quality validation  
  - Null checks
  - Duplicate removal
  - Anomaly detection
- Derived columns / calculated metrics
- Normalization or restructuring
- Analytical views for downstream usage

## Output

A **trusted, analysis-ready dataset**.

---

# 🥇 Gold Layer (Feature / Analytics Layer)

## Purpose

Build **feature-engineered datasets** aligned to the use case.

## Operations

- Feature engineering using SQL transformations
- Aggregations and behavioral metrics
- Analytical datasets for visualization or ML

---

# 🤖 ML Layer (AI/ML Inside Snowflake)

## Tool

**Snowpark (Python execution inside Snowflake compute)**

## Goal

- Train a **lightweight predictive or analytical model**
- Execute ML **directly inside Snowflake**
- Avoid moving data outside the platform
- Demonstrate **Data + AI convergence workflow**

---

# 📌 Why This Architecture Was Selected

This design:

- Handles **unknown schema datasets dynamically**
- Is **fast to implement within an 8-hour window**
- Matches **Snowflake enterprise best practices**
- Demonstrates capabilities across:
  - Data Engineering
  - Data Analytics
  - Feature Engineering
  - AI/ML Enablement

It avoids **overengineering while remaining scalable**.

---

# 📌 Sprint Strategy

## ✅ Sprint-1 — Foundation (Bronze Layer)

Focus **only on ingestion and validation**.

### Steps

1. Create **Database / Schema**
2. Create **Internal Stage**
3. Upload dataset (once provided)
4. Define reusable **File Format**
5. Use **INFER_SCHEMA** to detect dataset structure dynamically
6. Create **RAW table using inferred schema template**
7. Load data using **COPY INTO**
8. Validate ingestion with **row counts and sampling**
9. Create **STREAM for incremental change tracking**

### Deliverable

A **stable ingestion layer ready for transformations**.

---

## 🔜 Sprint-2 — Transformation Layer

After understanding dataset semantics:

- Apply **cleaning and restructuring logic**
- Build **derived datasets aligned to the problem statement**
- Create **transformation views or modeled tables**

---

## 🔜 Sprint-3 — Feature Engineering & Analytics

- Engineer **business or behavioral features**
- Produce **analytical datasets**
- Generate insights via **SQL and Snowsight**

---

## 🔜 Sprint-4 — AI/ML Integration

- Use **Snowpark** to access engineered datasets
- Train a **simple predictive / analytical model**
- Perform **in-database scoring or classification**

---

# 📌 Snowflake Capabilities Planned for Demonstration

The solution will showcase:

- **Internal Stages** for ingestion
- **File Formats** for reusable parsing
- **INFER_SCHEMA** for dynamic schema detection
- **COPY INTO** for scalable loading
- **Streams** for CDC / incremental workflows
- **Views** for logical transformation layers
- **SQL-based feature engineering**
- **Snowpark ML execution**

Optional governance features:

- Time Travel
- RBAC
- Cloning

---

# 📌 What This Project Is NOT

We are **NOT building**:

- External pipelines
- Kafka integrations
- Heavy orchestration frameworks
- Complex deep learning systems

This is an **agile Snowflake-native analytics + ML MVP**.

---

# 📌 Final Narrative to Present

The project demonstrates how to **rapidly operationalize an unknown dataset inside Snowflake** using:

- Schema inference
- Layered transformation
- In-database analytics
- Integrated ML

This allows organizations to **deliver insights quickly without moving data outside the platform**.

---

# 📌 Current Status

We are preparing to execute **Sprint-1 once the dataset is released**.

The workflow is designed to **immediately adapt to whatever structure is provided**.

This architecture also enables:

- Dynamic ingestion
- Rapid transformation
- Feature engineering
- ML experimentation

during the hackathon.

---

# 📊 Analytics Section

## 🔎 Step 1 — Confirm the Grain of Each KPI

Before writing SQL, define **what each KPI measures**.

| Requirement | Grain |
|-------------|------|
| REQ1 | Per Agent |
| REQ2 | Per State |
| REQ3 | Portfolio Level |
| REQ4 | City vs State |
| REQ5 | City within State |
| REQ6 | Customer Level |
| REQ7 | Customer Timeline |
| REQ8 | Dataset Comparison |

---
