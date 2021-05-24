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

-------------------------------------------------------------------
-- Supported Data-types
-------------------------------------------------------------------
CREATE FOREIGN TABLE datatype_test_tbl (
    empno           SMALLINT,
    ename           TEXT,
    job             TEXT,
    mgr             REAL,
    hiredate        pg_catalog.TIMESTAMP,
    sal             BIGINT,
    comm            SERIAL,
    deptno          DOUBLE PRECISION
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Test data-types: SMALLINT, BIGINT, SERIAL, REAL, DOUBLE PRECISION,
-- TIMESTAMP. Should pass.
SELECT empno, hiredate, sal, comm, mgr, deptno FROM datatype_test_tbl ORDER BY 1, 2, 3, 4, 5, 6;
DROP FOREIGN TABLE datatype_test_tbl;

CREATE FOREIGN TABLE datatype_test_tbl (
    empno           BYTEA,
    ename           TEXT,
    job             CHAR,
    mgr             INTEGER,
    hiredate        TIMESTAMP WITH TIME ZONE,
    sal             BIGINT,
    comm            VARCHAR,
    deptno          DOUBLE PRECISION
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Test data-types: BYTEA, VARCHAR, INTEGER, CHAR, TEXT, TIMESTAMP WITH TIME
-- ZONE. Should pass.
SELECT empno, hiredate, comm, mgr, job, ename FROM datatype_test_tbl ORDER BY 1, 2, 3, 4, 5, 6;
DROP FOREIGN TABLE datatype_test_tbl;

CREATE FOREIGN TABLE datatype_test_tbl (
    bol1           BOOLEAN,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

-- BOOLEAN data-type supported, should pass.
SELECT bol1 FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-------------------------------------------------------------------
-- Not Supported Data-types
-------------------------------------------------------------------
CREATE FOREIGN TABLE datatype_test_tbl (
    empno           MONEY,
    ename           TEXT,
    job             VARCHAR(9),
    mgr             NUMERIC,
    hiredate        INTERVAL,
    sal             CHAR(10),
    comm            INT,
    deptno          DECIMAL
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Test Non supported data-types: MONEY, NUMERIC, DECIMAL, INTERVAL
-- should errorout.
SELECT empno FROM datatype_test_tbl ORDER BY empno;
SELECT mgr FROM datatype_test_tbl ORDER BY empno;
SELECT deptno FROM datatype_test_tbl ORDER BY empno;
SELECT hiredate FROM datatype_test_tbl ORDER BY empno;
DROP FOREIGN TABLE datatype_test_tbl;

CREATE FOREIGN TABLE datatype_test_tbl (
    empno           CIDR,
    ename           INET,
    job             MACADDR,
    mgr             POINT,
    hiredate        LINE,
    sal             LSEG,
    comm            BOX,
    deptno          PATH
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Test Non supported data-types: CIDR, INET, MACADDR, POINT,
-- LINE, LSEG, BOX, PATH. Should errorout.
SELECT empno FROM datatype_test_tbl ORDER BY empno;
SELECT ename FROM datatype_test_tbl ORDER BY empno;
SELECT job FROM datatype_test_tbl ORDER BY empno;
SELECT mgr FROM datatype_test_tbl ORDER BY empno;
SELECT hiredate FROM datatype_test_tbl ORDER BY empno;
SELECT sal FROM datatype_test_tbl ORDER BY empno;
SELECT comm FROM datatype_test_tbl ORDER BY empno;
SELECT deptno FROM datatype_test_tbl ORDER BY empno;
DROP FOREIGN TABLE datatype_test_tbl;

CREATE TYPE deptnotype AS ENUM ('10', '20', '30', '40');
CREATE TYPE dnametype AS ENUM ('ACCOUNTING', 'RESEARCH','OPERATIONS','SALES');
CREATE FOREIGN TABLE datatype_test_tbl (
    empno           INT,
    ename           CIRCLE,
    job             TEXT[],
    mgr             TEXT ARRAY[3],
    hiredate        TEXT ARRAY,
    sal             TEXT,
    comm            dnametype,
    deptno          deptnotype
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

-- Test Non supported data-types: CIRCLE, ARRAY[], ARRAY[N], ARRAY, TEXT
-- should errorout.
SELECT ename FROM datatype_test_tbl ORDER BY empno;
SELECT job FROM datatype_test_tbl ORDER BY empno;
SELECT mgr FROM datatype_test_tbl ORDER BY empno;
SELECT hiredate FROM datatype_test_tbl ORDER BY empno;
SELECT comm FROM datatype_test_tbl ORDER BY empno;
SELECT deptno FROM datatype_test_tbl ORDER BY empno;
DROP FOREIGN TABLE datatype_test_tbl;
DROP TYPE dnametype;
DROP TYPE deptnotype;

-------------------------------------------------------------------
-- data-type mapping between compatible data-types
-------------------------------------------------------------------
CREATE FOREIGN TABLE datatype_test_tbl (
    empno           CHAR,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             VARCHAR(10),
    hiredate        TEXT,
    sal             TEXT,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');
-- CHAR mapped to INT in Hadoop, should pass
SELECT empno FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
-- VARCHAR2 mapped to INT in Hadoop, should pass
SELECT mgr FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
-- TEXT mapped to INT in Hadoop, should pass
SELECT sal FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
-- TEXT mapped to DATE, should pass.
SELECT hiredate FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- SMALLINT mapped to TINYINT, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          SMALLINT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- INT mapped to TINYINT, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          INT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- CHAR mapped to TINYINT, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          CHAR(2),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- VARCHAR2 mapped to TINYINT, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno             VARCHAR(10),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- TEXT mapped to BOOLEAN, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    bol1           TEXT,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- DATE mapped to STRING, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1            pg_catalog.DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');
SELECT pg_catalog.date(col1) FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIMESTAMP WITH TIME ZONE mapped to STRING, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1            TIMESTAMP WITH TIME ZONE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIME mapped to STRING, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1            pg_catalog.TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- DATE mapped to CHAR, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1           pg_catalog.DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');
SELECT pg_catalog.date(col1) FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIME mapped to CHAR, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1           pg_catalog.TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIMESTAMP mapped to CHAR, should pass.
CREATE FOREIGN TABLE datatype_test_tbl (
    col1           pg_catalog.TIMESTAMP
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');
SELECT * FROM datatype_test_tbl ORDER BY 1 LIMIT 1;
DROP FOREIGN TABLE datatype_test_tbl;

-------------------------------------------------------------------
-- data-type mapping between non-compatible data-types
-------------------------------------------------------------------
-- DATE mapped to INT, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          pg_catalog.DATE,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
DO
$$
BEGIN
  SELECT * FROM datatype_test_tbl ORDER BY deptno;
  EXCEPTION WHEN others THEN
	IF SQLERRM = 'invalid input syntax for type timestamp: "10"' OR
	   SQLERRM = 'invalid input syntax for type date: "10"' THEN
	   RAISE NOTICE 'ERROR:  invalid input syntax for type date: "10"';
        ELSE
	   RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIMESTAMP mapped to INT, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          pg_catalog.TIMESTAMP,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
SELECT * FROM datatype_test_tbl ORDER BY deptno;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIME mapped to INT, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          pg_catalog.TIME,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
SELECT * FROM datatype_test_tbl ORDER BY deptno;
DROP FOREIGN TABLE datatype_test_tbl;

-- DATE mapped to VARCHAR2, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          INT,
    dname           pg_catalog.DATE,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
DO
$$
BEGIN
  SELECT * FROM datatype_test_tbl ORDER BY deptno;
  EXCEPTION WHEN others THEN
	IF SQLERRM = 'invalid input syntax for type timestamp: "ACCOUNTING"' OR
	   SQLERRM = 'invalid input syntax for type date: "ACCOUNTING"' THEN
	   RAISE NOTICE 'ERROR:  invalid input syntax for type date: "ACCOUNTING"';
        ELSE
	   RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIMESTAMP mapped to VARCHAR2, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    deptno          INT,
    dname           pg_catalog.TIMESTAMP,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
SELECT * FROM datatype_test_tbl ORDER BY deptno;
DROP FOREIGN TABLE datatype_test_tbl;

CREATE FOREIGN TABLE datatype_test_tbl (
    empno           INTEGER,
    ename           BOOLEAN,
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        BOOLEAN,
    sal             BOOLEAN,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');
-- BOOLEAN mapped to DATE, should errorout.
SELECT hiredate FROM datatype_test_tbl ORDER BY empno;
-- BOOLEAN mapped to INT, should errorout.
SELECT sal FROM datatype_test_tbl ORDER BY empno;
-- BOOLEAN mapped to VARCHAR, should errorout.
SELECT ename FROM datatype_test_tbl ORDER BY empno;
DROP FOREIGN TABLE datatype_test_tbl;

-- DATE mapped to BOOLEAN, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    bol1           pg_catalog.DATE,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');
DO
$$
BEGIN
  SELECT * FROM datatype_test_tbl ORDER BY 1;
  EXCEPTION WHEN others THEN
	IF SQLERRM = 'invalid input syntax for type timestamp: "false"' OR
	   SQLERRM = 'invalid input syntax for type date: "false"' THEN
	   RAISE NOTICE 'ERROR:  invalid input syntax for type date: "false"';
        ELSE
	   RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;
DROP FOREIGN TABLE datatype_test_tbl;

-- TIME mapped to DATE, should errorout.
CREATE FOREIGN TABLE datatype_test_tbl (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        pg_catalog.TIME,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');
SELECT * FROM datatype_test_tbl ORDER BY empno;
DROP FOREIGN TABLE datatype_test_tbl;

--Cleanup
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
