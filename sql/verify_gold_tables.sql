USE DATABASE ABC;
USE SCHEMA POLICY_SCHEMA;

-- Row counts
SELECT 'GOLD_CUSTOMERS' AS TBL, COUNT(*) AS CNT FROM GOLD_CUSTOMERS
UNION ALL SELECT 'GOLD_ADDRESSES', COUNT(*) FROM GOLD_ADDRESSES
UNION ALL SELECT 'GOLD_AGENTS', COUNT(*) FROM GOLD_AGENTS
UNION ALL SELECT 'GOLD_POLICIES', COUNT(*) FROM GOLD_POLICIES
UNION ALL SELECT 'GOLD_CLAIMS', COUNT(*) FROM GOLD_CLAIMS;

-- FK integrity: policies → customers
SELECT 'orphan_policies_customer' AS CHECK_NAME,
       COUNT(*) AS VIOLATIONS
FROM GOLD_POLICIES p
LEFT JOIN GOLD_CUSTOMERS c ON p.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- FK integrity: policies → agents
SELECT 'orphan_policies_agent' AS CHECK_NAME,
       COUNT(*) AS VIOLATIONS
FROM GOLD_POLICIES p
LEFT JOIN GOLD_AGENTS a ON p.agent_id = a.agent_id
WHERE a.agent_id IS NULL;

-- FK integrity: addresses → customers
SELECT 'orphan_addresses' AS CHECK_NAME,
       COUNT(*) AS VIOLATIONS
FROM GOLD_ADDRESSES a
LEFT JOIN GOLD_CUSTOMERS c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Claims with valid policy link
SELECT 'claims_with_policy' AS CHECK_NAME,
       COUNT(*) AS CNT
FROM GOLD_CLAIMS cl
INNER JOIN GOLD_POLICIES p ON cl.policy_number = p.policy_number;

-- Claims without policy link (orphans)
SELECT 'claims_orphan' AS CHECK_NAME,
       COUNT(*) AS CNT
FROM GOLD_CLAIMS cl
LEFT JOIN GOLD_POLICIES p ON cl.policy_number = p.policy_number
WHERE p.policy_number IS NULL;

-- Sample customers
SELECT * FROM GOLD_CUSTOMERS LIMIT 3;

-- Sample claims
SELECT * FROM GOLD_CLAIMS LIMIT 3;
