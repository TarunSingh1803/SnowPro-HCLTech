USE DATABASE ABC;
USE SCHEMA POLICY_SCHEMA;

CREATE OR REPLACE TABLE RAW_CLAIMS (
    claim_id        VARCHAR,
    policy_number   VARCHAR,
    customer_id     VARCHAR,
    state           VARCHAR,
    city            VARCHAR,
    zip             VARCHAR,
    claim_type      VARCHAR,
    incident_date   VARCHAR,
    fnol_datetime   VARCHAR,
    status          VARCHAR,
    report_channel  VARCHAR,
    total_incurred  VARCHAR,
    total_paid      VARCHAR
);

COPY INTO RAW_CLAIMS (claim_id, policy_number, customer_id, state, city, zip, claim_type, incident_date, fnol_datetime, status, report_channel, total_incurred, total_paid)
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
    FROM @CLAIM_STAGES/claims_master.txt
)
FILE_FORMAT = (FORMAT_NAME = 'STREAM_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT * FROM RAW_CLAIMS LIMIT 5;
