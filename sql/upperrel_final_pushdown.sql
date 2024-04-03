\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

\c contrib_regression
SET hdfs_fdw.enable_order_by_pushdown TO ON;
CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server. log_remote_sql 'true' is required to setup logging
-- for Remote SQL Sent to Hive Server.
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
  OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', auth_type :AUTH_TYPE);

-- Create Hadoop USER MAPPING.
CREATE USER MAPPING FOR public SERVER hdfs_server
  OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SET hdfs_fdw.enable_order_by_pushdown TO ON;

-- Test OFFSET variations with a numeric value for LIMIT
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 2;
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET NULL;
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET NULL;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 0;
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 0;

-- Test OFFSET variations with LIMIT ALL
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT ALL;
SELECT empno FROM emp e ORDER BY empno LIMIT ALL;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT ALL OFFSET 1;
SELECT empno FROM emp e ORDER BY empno LIMIT ALL OFFSET 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT ALL OFFSET NULL;
SELECT empno FROM emp e ORDER BY empno LIMIT ALL OFFSET NULL;

-- Test OFFSET variations with LIMIT NULL
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT NULL;
SELECT empno FROM emp e ORDER BY empno LIMIT NULL;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT NULL OFFSET 1;
SELECT empno FROM emp e ORDER BY empno LIMIT NULL OFFSET 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT NULL OFFSET NULL;
SELECT empno FROM emp e ORDER BY empno LIMIT NULL OFFSET NULL;

-- Test LIMIT 0
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT 0;
SELECT empno FROM emp e ORDER BY empno LIMIT 0;

-- OFFSET without LIMIT is not pushed down when the remote server is Hive
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno OFFSET 1;
SELECT empno FROM emp e ORDER BY empno OFFSET 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno OFFSET NULL;
SELECT empno FROM emp e ORDER BY empno OFFSET NULL;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno OFFSET 0;
SELECT empno FROM emp e ORDER BY empno OFFSET 0;

-- LIMIT is not pushed down if the JOIN is not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.sal) FROM emp t1 INNER JOIN dept t2 ON (t1.deptno = t2.deptno) WHERE t1.sal * t2.deptno * random() <= 1 LIMIT 5;

-- Whole-row reference in ORDER BY, so upper rels are not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY emp LIMIT 5 OFFSET 1;
SELECT * FROM emp ORDER BY emp LIMIT 5 OFFSET 1;

-- Test LIMIT/OFFSET pushdown with enable_limit_pushdown GUC
SET hdfs_fdw.enable_limit_pushdown TO OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 2;
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 2;
SET hdfs_fdw.enable_limit_pushdown TO ON;

-- LIMIT/OFFSET scenarios eligible for pushdown
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5;
SELECT * FROM emp ORDER BY empno LIMIT 5;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT '5';
SELECT * FROM emp ORDER BY empno LIMIT '5';

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 1 + 1;
SELECT * FROM emp ORDER BY empno LIMIT 1 + 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 2;
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET '2';
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET '2';

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 1 + 1;
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 1 + 1;

-- Invalid values for LIMIT/OFFSET clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 'abc';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp ORDER BY empno LIMIT 5 OFFSET 'abc';

-- Test LIMIT/OFFSET clause with negative values
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp LIMIT -1;
SELECT * FROM emp LIMIT -1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp LIMIT 1 OFFSET -1;
SELECT * FROM emp LIMIT 1 OFFSET -1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp LIMIT -1 OFFSET -1;
SELECT * FROM emp LIMIT -1 OFFSET -1;

-- Test FOR UPDATE/SHARE clauses
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno for update;
SELECT empno FROM emp e ORDER BY empno for update;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno for share;
SELECT empno FROM emp e ORDER BY empno for share;

-- Test LIMIT/OFFSET with ORDER BY disabled
SET hdfs_fdw.enable_order_by_pushdown TO OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 2;
SELECT empno FROM emp e ORDER BY empno LIMIT 5 OFFSET 2;

-- Cleanup
SET hdfs_fdw.enable_order_by_pushdown TO OFF;
DROP FOREIGN TABLE emp;
DROP FOREIGN TABLE dept;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
