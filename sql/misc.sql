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

-- Check version
SELECT hdfs_fdw_version();

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- to verify that cost is not negative
EXPLAIN SELECT * FROM dept;
EXPLAIN SELECT * FROM dept WHERE deptno = 20;
EXPLAIN SELECT deptno FROM dept WHERE deptno BETWEEN 20 AND 30;

-- Verify that default fetch size is 10,000. This value will be sent to server log.
-- This can only be verified manually so following log is attached as example
-- where the default value 10000 is sent to server.
-- 2021-01-14 10:50:07 IST LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]
-- here [10000 is default size]
ALTER SERVER hdfs_server OPTIONS (log_remote_sql 'true');
SELECT * FROM dept ORDER BY deptno;

-- Change the value of fetch size.
-- Verify manually same in server log.
-- 2021-01-14 10:50:52 IST LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [200]
ALTER SERVER hdfs_server OPTIONS (fetch_size '200');
SELECT * FROM dept ORDER BY deptno;

-- Reset fetch_size to the default value.
-- Verify manually same in server log.
-- 2021-01-14 10:51:23 IST LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]
ALTER SERVER hdfs_server OPTIONS (DROP fetch_size);
SELECT * FROM dept ORDER BY deptno;

--Cleanup
DROP FOREIGN TABLE dept;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
