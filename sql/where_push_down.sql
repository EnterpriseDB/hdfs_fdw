/*---------------------------------------------------------------------------------------------------------------------
 *
 * mapping.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify the WHERE Clause with parameters is pushed down.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		where_push_down.sql
 *
 *---------------------------------------------------------------------------------------------------------------------
 */
\set enable_hashagg off

-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

CREATE DATABASE fdw_regression;
\c fdw_regression postgres

CREATE EXTENSION hdfs_fdw;

--==========================================================================================
--                    Pushing down parametrized WHERE clauses
-- 
-- Adds support for pushing down parametrized WHERE clauses.
--The query execution is broken down into three steps to make
--this possible: PREPARE, BIND and EXECUTE.
--While rescanning the new parameters are bound and the same
--prepared query is executed again.
--==========================================================================================

-- Create Hadoop FDW Server. log_remote_sql 'true' is required to setup logging for Remote SQL Sent to Hive Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', auth_type :AUTH_TYPE);

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.
-- Remote SQL: SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, ea.deptno FROM emp ea
WHERE sal = (SELECT MAX(sal) FROM emp eb 
	     WHERE eb.deptno = ea.deptno)
ORDER BY deptno;

-- The Server log will contain following info when this query will be executed, this rescan feature is tested manually as this cant be scripted.
--2017-10-17 04:46:11 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT empno, ename, sal, deptno FROM fdw_db.emp] [10000]
--2017-10-17 04:46:11 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:11 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [10000]
--2017-10-17 04:46:11 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:11 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [0]
--2017-10-17 04:46:11 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:11 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [1]
--2017-10-17 04:46:11 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [2]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [3]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [4]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [5]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [6]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [7]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [8]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:12 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [9]
--2017-10-17 04:46:12 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:13 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [10]
--2017-10-17 04:46:13 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:13 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [11]
--2017-10-17 04:46:13 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;
--2017-10-17 04:46:13 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))] [12]
--2017-10-17 04:46:13 EDT STATEMENT:  SELECT empno, ename, sal, deptno FROM emp e
--	WHERE  sal > (SELECT AVG(sal) FROM emp
--	              WHERE  emp.deptno = e.deptno)
--	ORDER BY deptno;

-- Expected output from Postgres is
--test=# SELECT empno, ename, sal, deptno FROM emp e
--test-# WHERE  sal > (SELECT AVG(sal) FROM emp
--test(#               WHERE  emp.deptno = e.deptno)
--test-# ORDER BY deptno;
-- empno | ename | sal  | deptno 
-------+-------+------+--------
--  7839 | KING  | 5000 |     10
--  7566 | JONES | 2975 |     20
--  7788 | SCOTT | 3000 |     20
--  7902 | FORD  | 3000 |     20
--  7499 | ALLEN | 1600 |     30
--  7698 | BLAKE | 2850 |     30
--(6 rows)
--
--test=# 


SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
              WHERE  emp.deptno = e.deptno)
ORDER BY deptno;

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.
-- Remote SQL: SELECT sal FROM fdw_db.emp WHERE ((deptno = ?))

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
              WHERE  emp.deptno = e.deptno)
ORDER BY deptno;

--Normal WHERE will not be parameterized.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT deptno "Department", COUNT(emp) "Total Employees" FROM emp GROUP BY deptno ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT deptno, SUM(sal) FROM emp GROUP BY deptno HAVING deptno IN (10, 30) ORDER BY deptno;

--********************************************************************
--Datatype Verification with parameterized WHERE Clause.
--********************************************************************
DROP FOREIGN TABLE emp;

--*******  INT2OID --> smallint  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             smallint,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT empno, ename, sal as "salary", deptno FROM emp e
WHERE  sal IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

SELECT empno, ename, sal as "salary", deptno FROM emp e
WHERE  sal NOT IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--*******  INT8OID --> bigint  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             bigint,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal NOT IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--*******  FLOAT4OID --> real  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             real,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--*******  FLOAT8OID --> double precision  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             double precision,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT sal FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT sal FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--*******  VARCHAROID --> VARCHAR  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT empno, ename, sal, deptno FROM emp e
WHERE  ename IN (SELECT ename FROM emp
              WHERE  emp.ename = e.ename)
ORDER BY deptno;

SELECT empno, ename, sal, deptno FROM emp e
WHERE  ename NOT IN (SELECT ename FROM emp
              WHERE  emp.ename = e.ename)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  ename NOT IN (SELECT ename FROM emp
              WHERE  emp.ename = e.ename)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--*******  INT4OID --> INTEGER  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.

SELECT ename FROM emp e
WHERE  empno IN (SELECT empno FROM emp
              WHERE  emp.empno = e.empno)
ORDER BY ename;

SELECT ename FROM emp e
WHERE  empno NOT IN (SELECT empno FROM emp
              WHERE  emp.empno = e.empno)
ORDER BY ename;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT ename FROM emp e
WHERE  empno NOT IN (SELECT empno FROM emp
              WHERE  emp.empno = e.empno)
ORDER BY ename;

DROP FOREIGN TABLE emp;

--******* DATEOID  --> DATE  ************
-- EXECPECT OUTPUT FROM POSTGRES
--test=# SELECT empno, ename, sal, deptno FROM emp e
--test-# WHERE  hiredate NOT IN (SELECT hiredate FROM emp
--test(#               WHERE  emp.deptno = e.deptno)
--test-# ORDER BY deptno;
-- empno | ename | sal | deptno 
-------+-------+-----+--------
--(0 rows)

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed.
SELECT comm FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
              WHERE  emp.hiredate = e.hiredate)
ORDER BY comm;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate NOT IN (SELECT hiredate FROM emp
              WHERE  emp.hiredate = e.hiredate)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--******* TIMESTAMPOID  --> TIMESTAMP  ************
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp2');

-- Execute the Query and the parametrized WHERE clause will be pushed.
SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
              WHERE  emp.hiredate = e.hiredate)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
              WHERE  emp.hiredate = e.hiredate)
ORDER BY deptno;

DROP FOREIGN TABLE emp;
--******* TIMESTAMPTZOID  --> TIMESTAMP WITH TIME ZONE  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP WITH TIME ZONE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp2');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
              WHERE  emp.hiredate = e.hiredate)
ORDER BY deptno;

DROP FOREIGN TABLE emp;

--******* TEXTOID  --> TEXT  ************

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP WITH TIME ZONE,
    sal             TEXT,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed.
SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT sal FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal NOT IN (SELECT sal FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

EXPLAIN (COSTS FALSE,VERBOSE TRUE)SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal IN (SELECT sal FROM emp
              WHERE  emp.sal = e.sal)
ORDER BY deptno;

--******* BPCHAROID  --> CHAR  ************

CREATE FOREIGN TABLE chr_date (
   col1              	char(30)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

-- Execute the Query and the parametrized WHERE clause will be pushed.
SELECT * FROM chr_date c
WHERE  col1 NOT IN (SELECT col1 FROM chr_date
              WHERE  chr_date.col1 = c.col1)
ORDER BY col1;

SELECT * FROM chr_date c
WHERE  col1 IN (SELECT col1 FROM chr_date
              WHERE  chr_date.col1 = c.col1)
ORDER BY col1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT * FROM chr_date c
WHERE  col1 NOT IN (SELECT col1 FROM chr_date
              WHERE  chr_date.col1 = c.col1)
ORDER BY col1;

--******* BOOLOID  -->  BOOLEAN ************

CREATE FOREIGN TABLE bool_test (
    bol1              	BOOLEAN,
    bol2                INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

-- Execute the Query and the parametrized WHERE clause will be pushed.
SELECT bol1,bol2 FROM bool_test b
WHERE  bol1 IN (SELECT bol1 FROM bool_test
              WHERE  bool_test.bol1 = b.bol1)
ORDER BY bol1;

SELECT bol1,bol2 FROM bool_test b
WHERE  bol1 NOT IN (SELECT bol1 FROM bool_test
              WHERE  bool_test.bol1 = b.bol1)
ORDER BY bol1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT bol1,bol2 FROM bool_test b
WHERE  bol1 IN (SELECT bol1 FROM bool_test
              WHERE  bool_test.bol1 = b.bol1)
ORDER BY bol1;

--******* JSONOID  -->  json ************

-- Error will be displayed as JSON Not supported.
CREATE FOREIGN TABLE json_test (
    bol1              	json,
    bol2                INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT bol1 FROM json_test;

--******* TIMEOID  -->  TIME ************

CREATE FOREIGN TABLE time_test (
    col1              	TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Error will be displayed as TIME is not handled by JDBC Driver.
SELECT col1 FROM time_test b
WHERE  col1 NOT IN (SELECT col1 FROM time_test
              WHERE  time_test.col1 = b.col1)
ORDER BY col1;

-- Plan will show column being pushed down
EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM time_test b
WHERE  col1 NOT IN (SELECT col1 FROM time_test
              WHERE  time_test.col1 = b.col1)
ORDER BY col1;

--******* NAMEOID  -->  NAME ************

CREATE FOREIGN TABLE name_test (
    col1              	NAME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Data will be returned and column wil be pushed.
SELECT col1 FROM name_test b
WHERE  col1 NOT IN (SELECT col1 FROM name_test
              WHERE  name_test.col1 = b.col1)
ORDER BY col1;

SELECT col1 FROM name_test b
WHERE  col1 IN (SELECT col1 FROM name_test
              WHERE  name_test.col1 = b.col1)
ORDER BY col1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM name_test b
WHERE  col1 NOT IN (SELECT col1 FROM name_test
              WHERE  name_test.col1 = b.col1)
ORDER BY col1;

--******* BITOID  -->  BIT ************

CREATE FOREIGN TABLE bit_test (
    col1              	bit
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Data will be returned and column wil be pushed.
SELECT col1 FROM bit_test b
WHERE  col1 NOT IN (SELECT col1 FROM bit_test
              WHERE  bit_test.col1 = b.col1)
ORDER BY col1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM bit_test b
WHERE  col1 NOT IN (SELECT col1 FROM bit_test
              WHERE  bit_test.col1 = b.col1)
ORDER BY col1;

--====================================================================
--                        RM # 42773
--====================================================================

DROP FOREIGN TABLE emp;

-- Verify that correct data is displayed.

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the parametrized WHERE clause will be pushed as shown here.


SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
               WHERE  emp.deptno = e.deptno AND emp.deptno > e.deptno)
ORDER BY deptno;


EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IN (SELECT hiredate FROM emp
               WHERE  emp.deptno = e.deptno AND emp.deptno > e.deptno)
ORDER BY deptno;

-- 2 Rows with empno 7900 and 7902 will be displayed.

SELECT empno, ename, sal, deptno FROM emp e WHERE hiredate IN (SELECT hiredate FROM emp WHERE emp.deptno < e.deptno OR emp.deptno > e.deptno);

-- 1 Row with empno 7902 will be displayed.

SELECT empno, ename, sal, deptno FROM emp e WHERE hiredate IN (SELECT hiredate FROM emp WHERE emp.empno < e.empno OR emp.deptno > e.deptno);




-- DROP EXTENSION
DROP EXTENSION hdfs_fdw CASCADE;
\c postgres postgres
DROP DATABASE fdw_regression;

