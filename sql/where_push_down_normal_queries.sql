/*---------------------------------------------------------------------------------------------------------------------
 *
 * mapping.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify the normal WHERE Clause pushed down.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		where_push_down_normal_queries.sql
 *
 *---------------------------------------------------------------------------------------------------------------------
 */

-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`

CREATE DATABASE fdw_regression;
\c fdw_regression postgres

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server. log_remote_sql 'true' is required to setup logging for Remote SQL Sent to Hive Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

--********************************************************************
--Datatype Verification with normal WHERE Clause pushdown.
--********************************************************************

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.
-- The condition will be shown in the Plan
--          Remote SQL: SELECT empno, ename, sal, deptno FROM fdw_db.emp WHERE (sal  IN (800,2450))

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno FROM emp e
WHERE  sal IN (800,2450)
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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.
--          Remote SQL: SELECT empno, ename, sal, deptno FROM fdw_db.emp WHERE ((sal > '3000'))

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > 3000 ORDER BY deptno;

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal = 2450 ORDER BY deptno;

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal BETWEEN 1000 AND 4000 ORDER BY deptno;

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT * FROM emp e
WHERE  ename IS NOT NULL ORDER BY deptno;

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT ename FROM emp e
WHERE  empno NOT IN (10,20)  ORDER BY ename;

DROP FOREIGN TABLE emp;

--******* DATEOID  --> DATE  ************
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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate <= '1980-12-17' ORDER BY deptno;

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
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IS NOT NULL ORDER BY deptno;

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
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  hiredate IS NULL ORDER BY deptno;

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

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE)SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal LIKE '300%' ORDER BY deptno;

--******* BPCHAROID  --> CHAR  ************

CREATE FOREIGN TABLE chr_date (
   col1              	char(30)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT * FROM chr_date c
WHERE  col1 = '10' ORDER BY col1;

--******* BOOLOID  -->  BOOLEAN ************

CREATE FOREIGN TABLE bool_test (
    bol1              	BOOLEAN,
    bol2                INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT bol1,bol2 FROM bool_test b
WHERE  bol1 = 'f' ORDER BY bol1;

--As per discussed with Dev, build in functions will not be pushed.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT bol1,bol2 FROM bool_test b
WHERE  bol1 = upper('t') ORDER BY bol1;

--******* TIMEOID  -->  TIME ************

CREATE FOREIGN TABLE time_test (
    col1              	TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM time_test b
WHERE  col1 IS NOT NULL ORDER BY col1;

--******* NAMEOID  -->  NAME ************

CREATE FOREIGN TABLE name_test (
    col1              	NAME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM name_test b
WHERE  col1 NOT IN ('a','b') ORDER BY col1;

--******* BITOID  -->  BIT ************

CREATE FOREIGN TABLE bit_test (
    col1              	bit
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Execute the Query and the normal WHERE clause will be pushed as shown here.

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1 FROM bit_test b
WHERE  col1 IS NULL ORDER BY col1;




-- DROP EXTENSION
DROP EXTENSION hdfs_fdw CASCADE;
DROP DATABASE fdw_regression;

