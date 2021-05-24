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
    hiredate        pg_catalog.DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Execute the query and the parameterized
-- WHERE clause will be pushed as shown here.
EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, ea.deptno FROM emp ea
WHERE sal = (SELECT MAX(sal) FROM emp eb
  WHERE eb.deptno = ea.deptno)
ORDER BY 1, 2, 3;

SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
  WHERE  emp.deptno = e.deptno)
ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno FROM emp e
WHERE  sal > (SELECT AVG(sal) FROM emp
  WHERE  emp.deptno = e.deptno)
ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- Datatype verification with parameterized WHERE Clause.
-- INT2OID --> smallint
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             smallint,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal NOT IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal >
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- INT8OID --> bigint
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             bigint,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal NOT IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal >
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- FLOAT4OID --> real
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             real,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal NOT IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE, VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal >
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- FLOAT8OID --> double precision
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             double precision,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal NOT IN
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal as "salary", deptno
 FROM emp e
 WHERE sal >
 (SELECT AVG(sal) FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- VARCHAROID --> VARCHAR
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE ename IN
 (SELECT ename FROM emp WHERE  emp.ename = e.ename)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE ename NOT IN
 (SELECT ename FROM emp WHERE  emp.ename = e.ename)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- DATEOID --> DATE

CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT comm
 FROM emp e
 WHERE hiredate IN
 (SELECT hiredate FROM emp WHERE  emp.hiredate = e.hiredate)
 ORDER BY comm;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT comm
 FROM emp e
 WHERE hiredate NOT IN
 (SELECT hiredate FROM emp WHERE  emp.hiredate = e.hiredate)
 ORDER BY comm;

DROP FOREIGN TABLE emp;

-- TIMESTAMPOID --> TIMESTAMP
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.TIMESTAMP,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp2');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE hiredate IN
 (SELECT hiredate FROM emp WHERE  emp.hiredate = e.hiredate)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE  hiredate IN
 (SELECT hiredate FROM emp WHERE  emp.hiredate = e.hiredate)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- TEXTOID --> TEXT
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             TEXT,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE sal IN
 (SELECT sal FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE sal NOT IN
 (SELECT sal FROM emp WHERE  emp.sal = e.sal)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;

-- BOOLOID -->  BOOLEAN ************
CREATE FOREIGN TABLE bool_test (
    bol1              	BOOLEAN,
    bol2                INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT bol1, bol2
 FROM bool_test b
 WHERE bol1 IN
 (SELECT bol1 FROM bool_test WHERE  bool_test.bol1 = b.bol1)
 ORDER BY 1, 2;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT bol1, bol2
 FROM bool_test b
 WHERE bol1 IN
 (SELECT bol1 FROM bool_test WHERE  bool_test.bol1 = b.bol1)
 ORDER BY 1, 2;

DROP FOREIGN TABLE bool_test;

-- JSONOID --> json
CREATE FOREIGN TABLE json_test (
    bol1              	json,
    bol2                INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

-- Error will be displayed as JSON Not supported.
SELECT bol1 FROM json_test;

DROP FOREIGN TABLE json_test;

-- TIMEOID --> TIME
CREATE FOREIGN TABLE time_test (
    col1              	TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Plan will show column being pushed down
EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1
 FROM time_test b
 WHERE col1 NOT IN (SELECT col1 FROM time_test WHERE  time_test.col1 = b.col1)
 ORDER BY col1;

-- Error will be displayed as TIME is not handled by JDBC Driver.
SELECT col1 FROM time_test b
 WHERE col1 NOT IN (SELECT col1 FROM time_test WHERE  time_test.col1 = b.col1)
 ORDER BY col1;

DROP FOREIGN TABLE time_test;

-- NAMEOID --> NAME
CREATE FOREIGN TABLE name_test (
    col1              	NAME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Data will be returned and column will be pushed.
EXPLAIN (COSTS FALSE,VERBOSE TRUE)  SELECT col1
 FROM name_test b
 WHERE col1 NOT IN
 (SELECT col1 FROM name_test WHERE  name_test.col1 = b.col1)
 ORDER BY col1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE)  SELECT col1
 FROM name_test b
 WHERE col1 IN
 (SELECT col1 FROM name_test WHERE  name_test.col1 = b.col1)
 ORDER BY col1;

DROP FOREIGN TABLE name_test;

-- BITOID --> BIT
CREATE FOREIGN TABLE bit_test (
    col1              	bit
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_date');

-- Data will be returned and column will be pushed.
EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1
 FROM bit_test b
 WHERE col1 NOT IN
 (SELECT col1 FROM bit_test WHERE  bit_test.col1 = b.col1)
 ORDER BY col1;

EXPLAIN (COSTS FALSE,VERBOSE TRUE) SELECT col1
 FROM bit_test b
 WHERE col1 IN
 (SELECT col1 FROM bit_test WHERE  bit_test.col1 = b.col1)
 ORDER BY col1;

DROP FOREIGN TABLE bit_test;

-- Verify that correct data is displayed.
CREATE FOREIGN TABLE emp (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.DATE,
    sal             VARCHAR(10),
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE hiredate IN
 (SELECT hiredate FROM emp WHERE emp.deptno < e.deptno OR emp.deptno > e.deptno)
 ORDER BY 1, 2, 3, 4;

SELECT empno, ename, sal, deptno
 FROM emp e
 WHERE hiredate IN
 (SELECT hiredate FROM emp WHERE emp.empno < e.empno OR emp.deptno > e.deptno)
 ORDER BY 1, 2, 3, 4;

DROP FOREIGN TABLE emp;
DROP FOREIGN TABLE dept;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
