# RBAC + Dynamic Data Masking — ABC Insurance

## Overview

Three roles control access to GOLD layer tables. Each role sees different rows and different levels of column masking.

| Role | User Type | Row Access | SSN Masking | Email/Phone |
|---|---|---|---|---|
| MANAGER_ROLE | Internal admin | All rows | Full (`534-12-5121`) | Full |
| AGENT_ROLE | Insurance agent | Only their assigned policies/claims | Partial (`***-**-5121`) | Partial |
| CUSTOMER_ROLE | Policyholder | Only their own data | Fully masked (`***-**-****`) | Own data unmasked |

---

## 1. Roles

Snowflake roles are permission groups. Privileges are granted to roles, then roles are assigned to users.

```
ACCOUNTADMIN
    └── SYSADMIN
           ├── MANAGER_ROLE    ← full read on all GOLD tables
           ├── AGENT_ROLE      ← filtered to own agent_id
           └── CUSTOMER_ROLE   ← filtered to own customer_id
```

### What each role can access

| Table | MANAGER_ROLE | AGENT_ROLE | CUSTOMER_ROLE |
|---|---|---|---|
| GOLD_CUSTOMERS | All rows | Only customers linked to their policies | Only own row |
| GOLD_ADDRESSES | All rows | Only addresses of their customers | Only own address |
| GOLD_AGENTS | All rows | Only own row | No access |
| GOLD_POLICIES | All rows | Only policies with their agent_id | Only their policies |
| GOLD_CLAIMS | All rows | Only claims on their policies | Only their claims |

---

## 2. User-Role Mapping Table

A mapping table links Snowflake login usernames to business IDs (customer_id or agent_id). Row access policies query this table at runtime.

```sql
CREATE TABLE USER_ROLE_MAPPING (
    snowflake_user   STRING NOT NULL,   -- Snowflake login username
    role_type        STRING NOT NULL,   -- 'MANAGER', 'AGENT', 'CUSTOMER'
    mapped_id        STRING,            -- customer_id or agent_id (NULL for manager)
    PRIMARY KEY (snowflake_user)
);
```

Example data:

| snowflake_user | role_type | mapped_id |
|---|---|---|
| MANAGER_USER1 | MANAGER | NULL |
| AGENT_USER1 | AGENT | AGT003 |
| CUST_USER1 | CUSTOMER | CIN1001 |

---

## 3. Row Access Policies

A Row Access Policy is a SQL function attached to a table that returns TRUE (show row) or FALSE (hide row) for each row, based on who is querying.

### How it works

1. User runs `SELECT * FROM GOLD_POLICIES`
2. Snowflake calls the row access policy for every row
3. Policy checks: does this user's mapped_id match the row's customer_id/agent_id?
4. Only matching rows are returned

### Policy logic per table

**GOLD_POLICIES:**
```
IF role = MANAGER_ROLE → return all rows
IF role = AGENT_ROLE   → return rows WHERE agent_id = user's mapped_id
IF role = CUSTOMER_ROLE → return rows WHERE customer_id = user's mapped_id
ELSE → return nothing
```

**GOLD_CLAIMS:**
```
IF role = MANAGER_ROLE → return all rows
IF role = AGENT_ROLE   → return rows WHERE policy_number IN (policies assigned to this agent)
IF role = CUSTOMER_ROLE → return rows WHERE customer_id = user's mapped_id
ELSE → return nothing
```

**GOLD_CUSTOMERS:**
```
IF role = MANAGER_ROLE → return all rows
IF role = AGENT_ROLE   → return rows WHERE customer_id IN (customers with policies assigned to this agent)
IF role = CUSTOMER_ROLE → return rows WHERE customer_id = user's mapped_id
ELSE → return nothing
```

### Key Snowflake functions used

| Function | Purpose |
|---|---|
| `CURRENT_ROLE()` | Returns active role of the session |
| `CURRENT_USER()` | Returns login username |
| `IS_ROLE_IN_SESSION('ROLE_NAME')` | Checks if role is active or inherited |

---

## 4. Dynamic Data Masking

A Masking Policy is a SQL function attached to a column that returns either the real value or a masked version, based on the querying role.

### SSN Column Masking

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `534-12-5121` | `534-12-5121` |
| AGENT_ROLE | `534-12-5121` | `***-**-5121` |
| CUSTOMER_ROLE | `534-12-5121` | `***-**-****` |

Logic:
```
IF role = MANAGER  → return full SSN
IF role = AGENT    → return '***-**-' || last 4 chars
IF role = CUSTOMER → return '***-**-****'
```

### Email Column Masking

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `aadhya.sharma@example.com` | `aadhya.sharma@example.com` |
| AGENT_ROLE | `aadhya.sharma@example.com` | `a****@example.com` |
| CUSTOMER_ROLE | `aadhya.sharma@example.com` | Own email: full. Others: masked |

### Phone Column Masking

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `+91 73119 40943` | `+91 73119 40943` |
| AGENT_ROLE | `+91 73119 40943` | `+91 **** 40943` |
| CUSTOMER_ROLE | `+91 73119 40943` | Own phone: full. Others: `**********` |

---

## 5. Implementation Order

Execution must follow this sequence due to dependencies:

```
Step 1: CREATE ROLES (MANAGER_ROLE, AGENT_ROLE, CUSTOMER_ROLE)
          ↓
Step 2: CREATE USER_ROLE_MAPPING table + populate
          ↓
Step 3: GRANT database/schema/warehouse usage to each role
          ↓
Step 4: GRANT SELECT on GOLD tables to each role
          ↓
Step 5: CREATE MASKING POLICIES (SSN, email, phone)
          ↓
Step 6: APPLY masking policies to columns (ALTER TABLE ... SET MASKING POLICY)
          ↓
Step 7: CREATE ROW ACCESS POLICIES (one per table)
          ↓
Step 8: APPLY row access policies to tables (ALTER TABLE ... ADD ROW ACCESS POLICY)
          ↓
Step 9: CREATE USERS + assign roles
          ↓
Step 10: TEST — login as each user, verify row filtering + masking
```

---

## 6. Constraints and Notes

- **One masking policy per column** — you cannot stack multiple policies on the same column.
- **One row access policy per table** — the policy function must handle all roles internally.
- **Row access policies are transparent** — users don't know they exist. They just see fewer rows.
- **Masking policies are also transparent** — users see masked values as if that's the real data.
- **Performance** — row access policies add a filter predicate. Snowflake optimizes this with micro-partition pruning.
- **ACCOUNTADMIN bypass** — ACCOUNTADMIN is NOT automatically exempt from row access or masking policies. You must explicitly handle it in the policy logic.
- **Policy ownership** — masking and row access policies should be owned by a governance role (e.g., SYSADMIN or a dedicated SECURITY_ADMIN role), not by the roles being restricted.

---

## 7. Testing Checklist

After deployment, validate with these tests:

| Test | Login As | Expected Result |
|---|---|---|
| Manager sees all policies | MANAGER_USER1 | 500 rows in GOLD_POLICIES |
| Agent sees only their policies | AGENT_USER1 | Only rows with agent_id = AGT003 |
| Customer sees only their policies | CUST_USER1 | Only rows with customer_id = CIN1001 |
| Manager sees full SSN | MANAGER_USER1 | `534-12-5121` |
| Agent sees partial SSN | AGENT_USER1 | `***-**-5121` |
| Customer sees masked SSN | CUST_USER1 | `***-**-****` |
| Agent cannot see GOLD_AGENTS (others) | AGENT_USER1 | Only own agent row |
| Customer cannot query GOLD_AGENTS | CUST_USER1 | Access denied or 0 rows |
