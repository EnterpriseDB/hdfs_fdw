/*-------------------------------------------------------------------------
 *
 * datatype.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify the datatype mappings functionality.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		datatype.sql
 *
 *-------------------------------------------------------------------------
 */


-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

-- Create the database.

CREATE DATABASE fdw_regression;

\c fdw_regression postgres

-- Set the Date Style

SET datestyle TO SQL,DMY;

SET TIME ZONE 'PST8PDT';

-- Set the Search Path to PUBLIC Schema

SET search_path TO public;

-- Create Hadoop FDW Extension.

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);


--=========================================================================================================
-- PG Not Supported Datatypes
-- Error message will be returned
-- ERROR:  unknown or unsupported PostgreSQL data type
-- HINT:  Supported data types are BOOL, INT, DATE, TIME, FLOAT, CHAR, TEXT and VARCHAR : 1186
--=========================================================================================================

-- Create Table with datatype CLOB, error message will be displayed.

CREATE FOREIGN TABLE emp_fr_21 (
 bol1           CLOB,
 bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_fr_21;

DROP FOREIGN TABLE emp_fr_21;

-- Create Table with datatype CLOB, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_clob (
    deptno          CLOB,
    dname           CLOB,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_clob;

DROP FOREIGN TABLE dept_dt_clob;

-- Create Table with datatype MONEY, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_mony (
    deptno          MONEY,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_mony;

DROP FOREIGN TABLE dept_dt_mony;

-- Create Table with datatype NUMERIC, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_num (
    deptno          NUMERIC,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_num;

DROP FOREIGN TABLE dept_dt_num;

-- Create Table with datatype decimal, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_dec (
    deptno          decimal,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_dec;

DROP FOREIGN TABLE dept_dt_dec;

-- Create Table with datatype interval, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_terv (
    deptno          interval,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_terv;

DROP FOREIGN TABLE dept_dt_terv;

-- Create Table with datatype cidr, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_cidr (
    deptno          cidr,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_cidr;

DROP FOREIGN TABLE dept_dt_cidr;

-- Create Table with datatype cidr, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_inet (
    deptno          inet,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_inet;

DROP FOREIGN TABLE dept_dt_inet;

-- Create Table with datatype macaddr, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_macaddr (
    deptno          macaddr,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_macaddr;

DROP FOREIGN TABLE dept_dt_macaddr;

-- Create Table with datatype point, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_point (
    deptno          point,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_point;

DROP FOREIGN TABLE dept_dt_point;

-- Create Table with datatype line, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_line (
    deptno          line,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_line;

DROP FOREIGN TABLE dept_dt_line;

-- Create Table with datatype lseg, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_lseg (
    deptno          lseg,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_lseg;

DROP FOREIGN TABLE dept_dt_lseg;

-- Create Table with box macaddr, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_box (
    deptno          box,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_box;

DROP FOREIGN TABLE dept_dt_box;

-- Create Table with datatype path, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_path (
    deptno          path,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_path;

DROP FOREIGN TABLE dept_dt_path;

-- Create Table with datatype polygon, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_polygon (
    deptno          polygon,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_polygon;

DROP FOREIGN TABLE dept_dt_polygon;

-- Create Table with datatype circle, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_circle (
    deptno          circle,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_circle;

DROP FOREIGN TABLE dept_dt_circle;

-- Create Table with array, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_arr (
    deptno          INT,
    dname           TEXT,
    loc             TEXT[3]
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_arr;

DROP FOREIGN TABLE dept_dt_arr;

CREATE FOREIGN TABLE dept_dt_arr1 (
    deptno          INT,
    dname           TEXT,
    loc             TEXT[]
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_arr1;

DROP FOREIGN TABLE dept_dt_arr1;

CREATE FOREIGN TABLE dept_dt_arr2 (
    deptno          INT,
    dname           TEXT,
    loc             TEXT ARRAY[3]
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_arr2;

DROP FOREIGN TABLE dept_dt_arr2;

CREATE FOREIGN TABLE dept_dt_arr3 (
    deptno          INT,
    dname           TEXT,
    loc             TEXT ARRAY
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_arr3;

DROP FOREIGN TABLE dept_dt_arr3;

-- Create Table with ENUM, error message will be displayed.

CREATE TYPE deptnotype AS ENUM ('10', '20', '30', '40');

CREATE FOREIGN TABLE dept_dt_enum (
    deptno          deptnotype,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

/*

Commenetd due to reported RM.

SELECT * FROM dept_dt_enum;

*/

DROP FOREIGN TABLE dept_dt_enum;

DROP TYPE deptnotype;

CREATE TYPE dnametype AS ENUM ('ACCOUNTING', 'RESEARCH','OPERATIONS','SALES');

CREATE FOREIGN TABLE dept_dt_enum1 (
    deptno          INT,
    dname           dnametype,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

/*

RM Reported.

SELECT * FROM dept_dt_enum1;


*/

DROP FOREIGN TABLE dept_dt_enum1;

DROP TYPE dnametype;

-- Create Table with datatype NUMERIC, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_nmric (
    deptno          NUMERIC,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_nmric;

DROP FOREIGN TABLE dept_dt_nmric;

-- Create Table with datatype NUMBER, error message will be displayed.

CREATE FOREIGN TABLE dept_dt_mp13 (
    deptno            NUMBER(2),
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp13;

DROP FOREIGN TABLE dept_dt_mp13;

--*************************************************************************

--===================================================================================
-- PG Not Supported Datatypes
-- Error message will be returned
-- Supported data types are BOOL, INT, DATE, TIME, FLOAT, CHAR, TEXT and VARCHAR
--===================================================================================

--=============================================================================================================================
-- HIVE Not Supported Datatypes
-- ERROR:  unknown or unsupported Hive data type
-- HINT:  Supported data types are TINYINT, SMALLINT, INT, BIGINT, STRING, CHAR, TIMESTAMPS, DECIMAL, DATE and VARCHAR: binary
--=============================================================================================================================

-- Create Hadoop Table with datatype binary, error message will be displayed.

CREATE FOREIGN TABLE binary_data
(
  col1         BLOB
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

DROP FOREIGN TABLE binary_data;

-- Create Hadoop Table with datatype DOUBLE, error message will be displayed.

CREATE FOREIGN TABLE dbl_data
(
  col1         FLOAT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'double_data');

SELECT * FROM dbl_data;

DROP FOREIGN TABLE dbl_data;

--=============================================================================================================================
-- HIVE Not Supported Datatypes
-- ERROR:  unknown or unsupported Hive data type
-- HINT:  Supported data types are TINYINT, SMALLINT, INT, BIGINT, STRING, CHAR, TIMESTAMPS, DECIMAL, DATE and VARCHAR: binary
--=============================================================================================================================

--===================================================================================
-- PG Supported Datatypes
-- Supported data types are BOOL, INT, DATE, TIME, FLOAT, CHAR, TEXT and VARCHAR
--===================================================================================

-- Create Table with datatype smallint, data will be displayed.

CREATE FOREIGN TABLE dept_dt_smlint (
    deptno          smallint,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_smlint ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_smlint;

-- Create Table with datatype bigint, data will be displayed.

CREATE FOREIGN TABLE dept_dt_bgint (
    deptno          bigint,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_bgint ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_bgint;

-- Create Table with datatype SERIAL, data will be displayed.

CREATE FOREIGN TABLE dept_dt_ser (
    deptno          SERIAL,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_ser ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_ser;

CREATE FOREIGN TABLE dept_dt_bgser (
    deptno          BIGSERIAL,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_bgser ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_bgser;

CREATE FOREIGN TABLE dept_dt_smlser (
    deptno          SMALLSERIAL,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_smlser ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_smlser;

-- Create Table with datatype REAL, data will be displayed.

CREATE FOREIGN TABLE dept_dt_real (
    deptno          REAL,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_real ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_real;

-- Create Table with datatype double precision, data will be displayed.

CREATE FOREIGN TABLE dept_dt_dbl (
    deptno          double precision,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_dbl ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_dbl;

-- Create Table with datatype BLOB, data will be displayed.

CREATE FOREIGN TABLE dept_dt_blob (
    deptno          BLOB,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_blob ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_blob;

-- Create Table with datatype BYTEA, data will be displayed.

CREATE FOREIGN TABLE dept_dt_byt (
    deptno          BYTEA,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_byt ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_byt;

-- Create Table with datatype STRING, data will be displayed.

CREATE FOREIGN TABLE dept_dt_str (
    deptno          STRING,
    dname           STRING,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_str ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_str;

-- Create Table with datatypes INTEGER and TEXTS, data will be displayed.

CREATE FOREIGN TABLE dept_dt_int (
    deptno          INTEGER,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_int ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_int;

-- Create Table with datatypes INT, CHAR and VARCHAR, data will be displayed.

CREATE FOREIGN TABLE dept_dt_var (
    deptno          INT,
    dname           CHAR(20),
    loc             VARCHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_var ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_var;

-- Create Table with datatype DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_date (
    empno           INT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INT,
    hiredate        DATE,
    sal             INT,
    comm            INT,
    deptno          INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_date ORDER BY deptno;

DROP FOREIGN TABLE emp_dt_date;

-- Create Table with datatype TIMESTAMP, data will be displayed.

CREATE FOREIGN TABLE emp_dt_time_tm (
    empno           INT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INT,
    hiredate        TIMESTAMP,
    sal             INT,
    comm            INT,
    deptno          INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_time_tm ORDER BY deptno;

DROP FOREIGN TABLE emp_dt_time_tm;

CREATE FOREIGN TABLE emp_dt_time (
    empno           INT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INT,
    hiredate        TIMESTAMP with time zone,
    sal             INT,
    comm            INT,
    deptno          INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_time ORDER BY empno;

DROP FOREIGN TABLE emp_dt_time;

CREATE FOREIGN TABLE emp_dt_time_otm (
    empno           INT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INT,
    hiredate        TIMESTAMP without time zone,
    sal             INT,
    comm            INT,
    deptno          INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_time_otm ORDER BY empno;

DROP FOREIGN TABLE emp_dt_time_otm;

-- Create Table with datatypes BOOL and BOOLEAN data will be displayed.

CREATE FOREIGN TABLE emp_dt_bol (
    bol1           BOOL,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bol ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bol;

CREATE FOREIGN TABLE emp_dt_bol1 (
    bol1           BOOLEAN,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bol1 ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bol1;

-- Create Table with datatypes CHAR mapped to INT in Hadoop, data will be displayed.

CREATE FOREIGN TABLE dept_dt_car (
    deptno          CHAR,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_car ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_car;

CREATE FOREIGN TABLE dept_dt_car1 (
    deptno          CHAR(10),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_car1 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_car1;

-- Create Table with datatypes VARCHAR2 mapped to INT in Hadoop, data will be displayed.

CREATE FOREIGN TABLE dept_dt_var2 (
    deptno          VARCHAR(10),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_var2 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_var2;

CREATE FOREIGN TABLE dept_dt_var3 (
    deptno          VARCHAR(10),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_var3 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_var3;

-- Create Table with datatypes TEXT mapped to INT in Hadoop, data will be displayed.

CREATE FOREIGN TABLE dept_dt_txt (
    deptno          TEXT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_txt ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_txt;

-- Mapp DATE with INT, error will be displayed.

CREATE FOREIGN TABLE dept_dt_dt (
    deptno          DATE,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_dt ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_dt;

-- Mapp TIMESTAMP with INT, error will be displayed.

CREATE FOREIGN TABLE dept_dt_tm (
    deptno          TIMESTAMP,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_tm ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_tm;

-- Mapp TIME with INT, error will be displayed.

CREATE FOREIGN TABLE dept_dt_tme (
    deptno          TIME,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_tme ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_tme;

-- Mapp INT with VARCHAR2, error will be displayed.

CREATE FOREIGN TABLE dept_dt_con_var (
    deptno          INT,
    dname           INT,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_con_var ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_con_var;

-- Mapp DATE with VARCHAR2, error will be displayed.

CREATE FOREIGN TABLE dept_dt_con_date (
    deptno          INT,
    dname           DATE,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_con_date ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_con_date;

-- Mapp TIMESTAMP with VARCHAR2, error will be displayed.

CREATE FOREIGN TABLE dept_dt_con_tmpstmp (
    deptno          INT,
    dname           TIMESTAMP,
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_con_tmpstmp ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_con_tmpstmp;

-- Data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp1 (
    deptno          INT,
    dname           CHAR(20),
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_mp1 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp1;

-- Data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp2 (
    deptno          INT,
    dname           CHAR,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt_mp2 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp2;

-- Data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp1 (
    empno           SMALLINT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             BIGINT,
    hiredate        DATE,
    sal             INT,
    comm            INT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp1 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp1;

-- Data will be displayed.

CREATE FOREIGN TABLE emp2 (
    empno           real,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             double precision,
    hiredate        DATE,
    sal             FLOAT,
    comm            FLOAT,
    deptno          FLOAT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp2 ORDER BY empno;

DROP FOREIGN TABLE emp2;

-- Error will be displayed.

CREATE FOREIGN TABLE dept_dt31 (
    deptno          INT,
    dname           real,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt31 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt31;

-- Error will be displayed.

CREATE FOREIGN TABLE dept_dt32 (
    deptno          INT,
    dname           double precision,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt32 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt32;

-- Mapp VARCHAR2 with DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp2 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        VARCHAR(10),
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp2 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp2;

-- Mapp CHAR with DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp3 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        CHAR,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp3 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp3;

-- Mapp CHAR with DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp4 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        CHAR(20),
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp4 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp4;

-- Mapp TEXT with DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp5 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        TEXT,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp5 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp5;

-- Mapp TIME with DATE, error will be displayed.

CREATE FOREIGN TABLE emp_dt_mp6 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	TIME,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp6 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp6;

-- Mapp TIMESTAMP with DATE, data will be displayed.

CREATE FOREIGN TABLE emp_dt_mp7 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	TIMESTAMP,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp7 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp7;

-- Mapp BOOLEAN with DATE, error will be displayed.

CREATE FOREIGN TABLE emp_dt_mp8 (
    empno           INTEGER,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	BOOLEAN,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp8 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp8;

-- Mapp BOOLEAN with INT, error will be displayed.

CREATE FOREIGN TABLE emp_dt_mp9 (
    empno           BOOLEAN,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp9 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp9;

-- Mapp BOOLEAN with VARCHAR, error will be displayed.

CREATE FOREIGN TABLE emp_dt_mp10 (
    empno           INT,
    ename           BOOLEAN,
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp10 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp10;

-- Mapp smallint with DATE, error will be displayed.

CREATE FOREIGN TABLE emp18 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	smallint,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp18 ORDER BY empno;

DROP FOREIGN TABLE emp18;

-- Mapp INTEGER with DATE, error will be displayed.

CREATE FOREIGN TABLE emp_dt_mp11 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	INTEGER,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp_dt_mp11 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_mp11;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp20 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	bigint,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp20 ORDER BY empno;

DROP FOREIGN TABLE emp20;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp23 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	real,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp23 ORDER BY empno;

DROP FOREIGN TABLE emp23;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp24 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	double precision,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp24 ORDER BY empno;

DROP FOREIGN TABLE emp24;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp25 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	smallserial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp25 ORDER BY empno;

DROP FOREIGN TABLE emp25;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp26 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	serial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp26 ORDER BY empno;

DROP FOREIGN TABLE emp26;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp27 (
    empno           INT,
    ename           VARCHAR(20),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate       	bigserial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp27 ORDER BY empno;

DROP FOREIGN TABLE emp27;

-- Mapp INTEGER with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp1 (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp1 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp1;

-- Mapp SMALLINT with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_2 (
    deptno          SMALLINT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp_2 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_2;

-- Mapp INT with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp3 (
    deptno          INT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp3 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp3;

-- Mapp CHAR with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp4 (
    deptno          CHAR(2),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp4 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp4;

-- Mapp VARCHAR2 with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp5 (
    deptno             VARCHAR(10),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp5 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp5;

-- Mapp bigint with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp7 (
    deptno            bigint,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp7 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp7;

-- Mapp INT with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp8 (
    deptno            INT,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp8 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp8;

-- Mapp INTEGER with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_8 (
    deptno            INTEGER,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_8 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_8;

-- Mapp BIGINT with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_9 (
    deptno            BIGINT,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_9 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_9;

-- Mapp SMALLINT with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_10 (
    deptno            SMALLINT,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_10 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_10;

-- Mapp CHAR with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_11 (
    deptno            CHAR(5),
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_11 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_11;

-- Mapp VARCHAR with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_12 (
    deptno            VARCHAR(5),
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_12 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_12;

-- Mapp TEXT with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_13 (
    deptno            TEXT,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_13 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_13;

-- Mapp CHAR with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_10 (
    deptno          CHAR,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp_10 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp_10;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp9 (
    deptno            SMALLINT,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp9 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp9;

-- Mapp REAL with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp10 (
    deptno            real,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp10 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp10;

-- Mapp double precision with BIGINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp12 (
    deptno            double precision,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp12 ORDER BY deptno;

DROP FOREIGN TABLE dept_dt_mp12;

-- Mapp TEXT with BOOLEAN, data will be displayed.

CREATE FOREIGN TABLE emp_dt_bool1 (
    bol1           TEXT,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bool1 ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bool1;

-- Mapp INT with BOOLEAN, error will be displayed.

CREATE FOREIGN TABLE emp_dt_bool2 (
    bol1           INT,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bool2 ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bool2;

-- Mapp VARCHAR2 with BOOLEAN, data will be displayed.

CREATE FOREIGN TABLE emp_dt_bool3 (
    bol1           VARCHAR(6),
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bool3 ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bool3;

-- Mapp CHAR with BOOLEAN, data will be displayed.

CREATE FOREIGN TABLE emp_dt_bool4 (
    bol1           CHAR(6),
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_dt_bool4 ORDER BY bol1;

DROP FOREIGN TABLE emp_dt_bool4;

-- Mapp BLOB with BOOLEAN, data will be displayed.

CREATE FOREIGN TABLE emp_bol5 (
    bol1             BLOB,
    bol2             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol5 ORDER BY bol1;

DROP FOREIGN TABLE emp_bol5;

-- Mapp DATE with BOOLEAN, data will be displayed.

CREATE FOREIGN TABLE emp_bol8 (
    bol1            DATE,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol8 ORDER BY bol1;

DROP FOREIGN TABLE emp_bol8;

-- Mapp DATE with STRING, data will be displayed.

CREATE FOREIGN TABLE str_date (
    col1            DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date ORDER BY col1;

DROP FOREIGN TABLE str_date;

-- Mapp timestamp with time zone with STRING, data will be displayed.

CREATE FOREIGN TABLE str_date1 (
    col1            timestamp with time zone 
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date1 ORDER BY col1;

DROP FOREIGN TABLE str_date1;

-- Mapp TIME with STRING, data will be displayed.

CREATE FOREIGN TABLE str_date2 (
    col1            TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date2 ORDER BY col1;

DROP FOREIGN TABLE str_date2;

-- Mapp DATE with CHAR, data will be displayed.

CREATE FOREIGN TABLE chr_date1 (
    col1           DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date1 ORDER BY col1;

DROP FOREIGN TABLE chr_date1;

-- Mapp TIME with CHAR, data will be displayed.

CREATE FOREIGN TABLE chr_date2 (
    col1           TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date2 ORDER BY col1;

DROP FOREIGN TABLE chr_date2;

-- Mapp TIMESTAMP with CHAR, data will be displayed.

CREATE FOREIGN TABLE chr_date3 (
    col1           TIMESTAMP
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date3 ORDER BY col1;

DROP FOREIGN TABLE chr_date3;

-- Mapp INT with VARCHAR, data will be displayed.

CREATE FOREIGN TABLE var_int (
    col1           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_int');

SELECT col1+1 FROM var_int ORDER BY col1;

DROP FOREIGN TABLE var_int;

-- Mapp INT with STRING, data will be displayed.

CREATE FOREIGN TABLE str_int (
    col1           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_int');

SELECT col1+1 FROM str_int ORDER BY col1;

DROP FOREIGN TABLE str_int;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_dt_1 (
    empno           SMALLINT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             INTEGER,
    comm            INTEGER	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_dt_1 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_1;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_dt_2 (
    empno           SMALLINT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             INTEGER,
    comm            INTEGER	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_dt_2 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_2;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr4 (
    empno           bigint,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        INTERVAL,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr4 ORDER BY empno;

DROP FOREIGN TABLE emp_fr4;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr5 (
    empno           bigint,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr5 ORDER BY empno;

DROP FOREIGN TABLE emp_fr5;


/*

RM # 37619 Logged

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr3 (
    empno           INT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             INTEGER,
    comm            INTEGER	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr3;

DROP FOREIGN TABLE emp_fr3;
*/

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr6 (
    empno           REAL,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        timestamp with time zone,
    sal             double precision,
    comm            double precision	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr6 ORDER BY empno;

DROP FOREIGN TABLE emp_fr6;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr7 (
    empno           double precision,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        timestamp without time zone,
    sal             double precision,
    comm            double precision	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr7 ORDER BY empno;

DROP FOREIGN TABLE emp_fr7;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr12 (
    empno           TEXT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        CHAR(30),
    sal             FLOAT,
    comm            FLOAT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr12 ORDER BY empno;

DROP FOREIGN TABLE emp_fr12;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr13 (
    empno           VARCHAR(10),
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             FLOAT,
    comm            FLOAT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr13 ORDER BY empno;

DROP FOREIGN TABLE emp_fr13;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr14 (
    empno           CHAR(10),
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             FLOAT,
    comm            FLOAT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr14 ORDER BY empno;

DROP FOREIGN TABLE emp_fr14;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fro14 (
    empno           FLOAT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             FLOAT,
    hiredate        DATE,
    sal             FLOAT,
    comm            FLOAT,
    deptno          FLOAT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fro14 ORDER BY empno;

DROP FOREIGN TABLE emp_fro14;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr15 (
    empno           DATE,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             FLOAT,
    comm            FLOAT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr15 ORDER BY empno;

DROP FOREIGN TABLE emp_fr15;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr16 (
    empno           TEXT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             FLOAT,
    comm            VARCHAR(9),
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr16 ORDER BY empno;

DROP FOREIGN TABLE emp_fr16;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr17 (
    empno           TEXT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR(9),
    comm            VARCHAR(9),
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr17 ORDER BY empno;

DROP FOREIGN TABLE emp_fr17;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr18 (
    empno           TEXT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             CHAR(10),
    comm            TEXT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr18 ORDER BY empno;

DROP FOREIGN TABLE emp_fr18;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr19 (
    empno           TEXT,
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             BLOB,
    comm            BLOB,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr19 ORDER BY empno;

DROP FOREIGN TABLE emp_fr19;

/*

RM # 38240 Logged

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float1 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             FLOAT,
    comm            FLOAT	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float1;

DROP FOREIGN TABLE emp_float1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float2 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             double precision,
    comm            FLOAT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float2;

DROP FOREIGN TABLE emp_float2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float3 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            text,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float3;

DROP FOREIGN TABLE emp_float3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float4 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            VARCHAR2(9),
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float4;

DROP FOREIGN TABLE emp_float4;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float5 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            CHAR(9),
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float5;

DROP FOREIGN TABLE emp_float5;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float6 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            INT,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float6;

DROP FOREIGN TABLE emp_float6;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float7 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            smallint,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float7;

DROP FOREIGN TABLE emp_float7;


-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float8 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            bigint,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float8;

DROP FOREIGN TABLE emp_float8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float9 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            decimal,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float9;

DROP FOREIGN TABLE emp_float9;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float10 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            BLOB,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float10;

DROP FOREIGN TABLE emp_float10;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float11 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            CLOB,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float11;

DROP FOREIGN TABLE emp_float11;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float12 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            real,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float12;

DROP FOREIGN TABLE emp_float12;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_float13 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            NUMBER(2),
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float13;

DROP FOREIGN TABLE emp_float13;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_float14 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal            DATE,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp2');

SELECT * FROM emp_float14;

DROP FOREIGN TABLE emp_float14;

*/

-- Mapp CHAR(0) with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_4 (
    deptno          CHAR(0),
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

/*
RM # 38290  Reported

-- Mapp CHAR(1) with TINYINT, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp_41 (
    deptno          CHAR(1),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp_41;

DROP FOREIGN TABLE dept_dt_mp_41;

CREATE FOREIGN TABLE dept_dt_mp_42 (
    deptno          CHAR(1),
    dname           VARCHAR2(1),
    loc             VARCHAR2(1)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp_42;

DROP FOREIGN TABLE dept_dt_mp_42;
*/

--*************************************************************************

-- Mapp DECIMAL with BIGINT, error will be displayed.

CREATE FOREIGN TABLE emp_dt_bg1 (
    empno           VARCHAR(10),
    ename           VARCHAR(10),
    job             VARCHAR(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             FLOAT,
    comm            BIGINT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_dt_bg1 ORDER BY empno;

DROP FOREIGN TABLE emp_dt_bg1;

--===================================================================================
-- PG Supported Datatypes
-- Supported data types are BOOL, INT, DATE, TIME, FLOAT, CHAR, TEXT and VARCHAR
--===================================================================================

/*-- RM # 37686

-- Create the Table.

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

-- Query the Table.

SELECT * FROM weblogs
LIMIT 500000;


-- Cleanup

DROP FOREIGN TABLE weblogs;*/



--Cleanup

DROP EXTENSION hdfs_fdw CASCADE;

\c postgres postgres

DROP DATABASE fdw_regression;
