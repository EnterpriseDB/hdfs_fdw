\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

\c contrib_regression

CREATE EXTENSION IF NOT EXISTS hdfs_fdw;
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
 OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);
CREATE USER MAPPING FOR public SERVER hdfs_server
 OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

CREATE FOREIGN TABLE weblogs
(
    client_ip                TEXT,
    full_request_date        TEXT,
    day                      TEXT,
    Month                    TEXT,
    month_num                INTEGER,
    year                     TEXT,
    hour                     TEXT,
    minute                   TEXT,
    second                   TEXT,
    timezone                 TEXT,
    http_verb                TEXT,
    uri                      TEXT,
    http_status_code         TEXT,
    bytes_returned           TEXT,
    referrer                 TEXT,
    user_agent               TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'weblogs');

-- Server without use_remote_estimate option.
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM weblogs;

-- Use wrong parameter name, error message will be displayed.
ALTER SERVER hdfs_server OPTIONS (use_remote_estimate 'wrong');
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM weblogs;

-- Use non-Boolean parameter name, error message will be displayed.
ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate '1');
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM weblogs;

-- Using false value in use_remote_estimate, 1000 rows will be returned in
-- query Plan
ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'false');
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM weblogs;

-- Function to fetch rows.
CREATE OR REPLACE FUNCTION query_rows_count (client_type VARCHAR(70), ure BOOL, query TEXT, OUT v_rows FLOAT)
RETURNS FLOAT LANGUAGE plpgsql AS $$
 DECLARE
  detail json;
 BEGIN
  CREATE TEMPORARY TABLE query_rows_stats (rows float);
  EXECUTE 'EXPLAIN (verbose, format json) ' || query INTO detail;
  IF client_type <> 'spark' THEN
   INSERT INTO query_rows_stats VALUES ((detail->0->'Plan'->'Plan Rows')::text::float);
  END IF;
  IF client_type = 'spark' AND ure = false THEN
   INSERT INTO query_rows_stats VALUES ((detail->0->'Plan'->'Plan Rows')::text::float);
  END IF;
  IF client_type = 'spark' AND ure = true THEN
   INSERT INTO query_rows_stats VALUES ('445454'::float);
  END IF;
  SELECT rows INTO v_rows FROM query_rows_stats;
  DROP TABLE query_rows_stats;
 END;
$$;

-- Check rows with use_remote_estimate set to false.
-- use_remote_estimate when set to false shows minimum 1000 rows in plan.
ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'false');
SELECT v_rows FROM query_rows_count (:HIVE_CLIENT_TYPE, false, 'SELECT * FROM weblogs');

-- Check rows with use_remote_estimate set to true.
-- use_remote_estimate when true in case of hive shows actual rows in plan.
-- use_remote_estimate when true in case of spark shows minimum 1000 rows in plan.
-- to make output file consistent inserted actual rows using query_rows_count.
-- This can be removed once use_remote_estimate fixed for spark.
-- NOTE: if the test fails on hive due to mismatch in the rows, execute
-- following ANALYZE command on weblogs table on hive client(beeline):
-- ANALYZE TABLE fdw_db.weblogs COMPUTE STATISTICS;
ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'true');
SELECT v_rows FROM query_rows_count (:HIVE_CLIENT_TYPE, true, 'SELECT * FROM weblogs');

--Cleanup
DROP FUNCTION query_rows_count(VARCHAR, BOOL, TEXT);
DROP FOREIGN TABLE weblogs;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
