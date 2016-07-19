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

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(port '10000', client_type 'hiveserver2');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt (
    deptno          CHAR,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt;

DROP FOREIGN TABLE dept_dt;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt1 (
    deptno          CHAR(10),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt1;

DROP FOREIGN TABLE dept_dt1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt2 (
    deptno          VARCHAR2(10),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt2;

DROP FOREIGN TABLE dept_dt2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt3 (
    deptno          VARCHAR(10),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt3;

DROP FOREIGN TABLE dept_dt3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt4 (
    deptno          TEXT,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt4;

DROP FOREIGN TABLE dept_dt4;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt5 (
    deptno          DATE,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt5;

DROP FOREIGN TABLE dept_dt5;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt6 (
    deptno          TIMESTAMP,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt6;

DROP FOREIGN TABLE dept_dt6;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt7 (
    deptno          TIME,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt7;

DROP FOREIGN TABLE dept_dt7;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt8 (
    deptno          INTERVAL,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt8;

DROP FOREIGN TABLE dept_dt8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt9 (
    deptno          BLOB,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt9;

DROP FOREIGN TABLE dept_dt9;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt10 (
    deptno          INT,
    dname           INT,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt10;

DROP FOREIGN TABLE dept_dt10;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt11 (
    deptno          CLOB,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt11;

DROP FOREIGN TABLE dept_dt11;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt12 (
    deptno          INT,
    dname           DATE,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt12;

DROP FOREIGN TABLE dept_dt12;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt13 (
    deptno          INT,
    dname           TIMESTAMP,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt13;

DROP FOREIGN TABLE dept_dt13;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt14 (
    deptno          INT,
    dname           INTERVAL,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt14;

DROP FOREIGN TABLE dept_dt14;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt15 (
    deptno          INT,
    dname           CHAR(20),
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt15;

DROP FOREIGN TABLE dept_dt15;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt16 (
    deptno          INT,
    dname           CHAR,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt16;

DROP FOREIGN TABLE dept_dt16;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt17 (
    deptno          INT,
    dname           BLOB,
    loc             CLOB
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt17;

DROP FOREIGN TABLE dept_dt17;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt18 (
    deptno          MONEY,
    dname           MONEY,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt18;

DROP FOREIGN TABLE dept_dt18;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt19 (
    deptno          MONEY,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt19;

DROP FOREIGN TABLE dept_dt19;

CREATE FOREIGN TABLE dept_dt20 (
    deptno          BYTEA,
    dname           BYTEA,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt20;

DROP FOREIGN TABLE dept_dt20;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt21 (
    deptno          INT,
    dname           smallserial,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt21;

DROP FOREIGN TABLE dept_dt21;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt22 (
    deptno          INT,
    dname           serial,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt22;

DROP FOREIGN TABLE dept_dt22;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt23 (
    deptno          INT,
    dname           bigserial,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt23;

DROP FOREIGN TABLE dept_dt23;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt24 (
    deptno          smallserial,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt24;

DROP FOREIGN TABLE dept_dt24;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt25 (
    deptno          serial,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt25;

DROP FOREIGN TABLE dept_dt25;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt26 (
    deptno          bigserial,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt26;

DROP FOREIGN TABLE dept_dt26;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp1 (
    empno           smallint,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             bigint,
    hiredate        DATE,
    sal             decimal,
    comm            numeric,
    deptno          numeric(4,2)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp1;

DROP FOREIGN TABLE emp1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp2 (
    empno           real,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             double precision,
    hiredate        DATE,
    sal             decimal,
    comm            numeric,
    deptno          numeric(4,2)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp2;

DROP FOREIGN TABLE emp2;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt27 (
    deptno          INT,
    dname           bigint,
    loc             decimal
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt27;

DROP FOREIGN TABLE dept_dt27;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt28 (
    deptno          INT,
    dname           TEXT,
    loc             decimal
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt28;

DROP FOREIGN TABLE dept_dt28;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt29 (
    deptno          INT,
    dname           numeric,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt29;

DROP FOREIGN TABLE dept_dt29;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt30 (
    deptno          INT,
    dname           numeric(4,2),
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt30;

DROP FOREIGN TABLE dept_dt30;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt31 (
    deptno          INT,
    dname           real,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt31;

DROP FOREIGN TABLE dept_dt31;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt32 (
    deptno          INT,
    dname           double precision,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_dt32;

DROP FOREIGN TABLE dept_dt32;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp4 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        VARCHAR2(10),
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp4;

DROP FOREIGN TABLE emp4;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp5 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        CHAR,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp5;

DROP FOREIGN TABLE emp5;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp6 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        CHAR(20),
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp6;

DROP FOREIGN TABLE emp6;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp7 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TEXT,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp7;

DROP FOREIGN TABLE emp7;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp8 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	CLOB,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp8;

DROP FOREIGN TABLE emp8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp9 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	BLOB,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp9;

DROP FOREIGN TABLE emp9;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp10 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	TIME,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp10;

DROP FOREIGN TABLE emp10;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp11 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	INTERVAL,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp11;

DROP FOREIGN TABLE emp11;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp12 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	TIMESTAMP,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp12;

DROP FOREIGN TABLE emp12;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp13 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	MONEY,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp13;

DROP FOREIGN TABLE emp13;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp14 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	BYTEA,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp14;

DROP FOREIGN TABLE emp14;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp15 (
    empno           INTEGER,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	BOOLEAN,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp15;

DROP FOREIGN TABLE emp15;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp16 (
    empno           BOOLEAN,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp16;

DROP FOREIGN TABLE emp16;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp17 (
    empno           INT,
    ename           BOOLEAN,
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	DATE,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp17;

DROP FOREIGN TABLE emp17;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp18 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	smallint,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp18;

DROP FOREIGN TABLE emp18;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp19 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	INTEGER,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp19;

DROP FOREIGN TABLE emp19;


-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp20 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	bigint,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp20;

DROP FOREIGN TABLE emp20;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp21 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	decimal,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp21;

DROP FOREIGN TABLE emp21;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp22 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	numeric,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp22;

DROP FOREIGN TABLE emp22;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp23 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	real,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp23;

DROP FOREIGN TABLE emp23;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp24 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	double precision,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp24;

DROP FOREIGN TABLE emp24;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp25 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	smallserial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp25;

DROP FOREIGN TABLE emp25;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp26 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	serial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp26;

DROP FOREIGN TABLE emp26;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp27 (
    empno           INT,
    ename           VARCHAR2(20),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate       	bigserial,
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp');

SELECT * FROM emp27;

DROP FOREIGN TABLE emp27;

-- Create Foreign Table, data will be displayed.

CREATE TYPE deptnotype AS ENUM ('10', '20', '30', '40');

CREATE FOREIGN TABLE d1 (
    deptno          deptnotype,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM d1;

DROP FOREIGN TABLE d1;

DROP TYPE deptnotype;

-- Create Foreign Table, data will be displayed.

CREATE TYPE dnametype AS ENUM ('ACCOUNTING', 'RESEARCH','OPERATIONS','SALES');

CREATE FOREIGN TABLE d2 (
    deptno          INT,
    dname           dnametype,
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM d2;

DROP FOREIGN TABLE d2;

DROP TYPE dnametype;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt_mp1 (
    deptno          INTEGER,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp1;

DROP FOREIGN TABLE dept_dt_mp1;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt_mp2 (
    deptno          smallint,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp2;

DROP FOREIGN TABLE dept_dt_mp2;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_dt_mp3 (
    deptno          INT,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp3;

DROP FOREIGN TABLE dept_dt_mp3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp4 (
    deptno          CHAR(2),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp4;

DROP FOREIGN TABLE dept_dt_mp4;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp5 (
    deptno             VARCHAR2(10),
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

SELECT * FROM dept_dt_mp5;

DROP FOREIGN TABLE dept_dt_mp5;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp7 (
    deptno            bigint,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp7;

DROP FOREIGN TABLE dept_dt_mp7;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp8 (
    deptno            INT,
    dname           TEXT,
    loc             TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp8;

DROP FOREIGN TABLE dept_dt_mp8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp9 (
    deptno            SMALLINT,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp9;

DROP FOREIGN TABLE dept_dt_mp9;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp10 (
    deptno            real,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp10;

DROP FOREIGN TABLE dept_dt_mp10;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp11 (
    deptno            decimal,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp11;

DROP FOREIGN TABLE dept_dt_mp11;


-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp12 (
    deptno            double precision,
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp12;

DROP FOREIGN TABLE dept_dt_mp12;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE dept_dt_mp13 (
    deptno            NUMBER(2),
    dname           CHAR(20),
    loc             CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp1');

SELECT * FROM dept_dt_mp13;

DROP FOREIGN TABLE dept_dt_mp13;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr1 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr1;

DROP FOREIGN TABLE emp_fr1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr2 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr2;

DROP FOREIGN TABLE emp_fr2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr3 (
    empno           INT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIME,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr3;

DROP FOREIGN TABLE emp_fr3;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr4 (
    empno           bigint,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        INTERVAL,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr4;

DROP FOREIGN TABLE emp_fr4;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr5 (
    empno           bigint,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr5;

DROP FOREIGN TABLE emp_fr5;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr6 (
    empno           REAL,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        timestamp with time zone,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr6;

DROP FOREIGN TABLE emp_fr6;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr7 (
    empno           double precision,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        timestamp without time zone,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr7;

DROP FOREIGN TABLE emp_fr7;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr8 (
    empno           NUMBER(2),
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        timestamp without time zone,
    sal             double precision,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr8;

DROP FOREIGN TABLE emp_fr8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr9 (
    empno           DECIMAL(7,2),
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TEXT,
    sal             smallint,
    comm            DECIMAL(7,2)	,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr9;

DROP FOREIGN TABLE emp_fr9;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr10 (
    empno           DECIMAL(7,2),
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        VARCHAR2(30),
    sal             INT,
    comm            DECIMAL,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr10;

DROP FOREIGN TABLE emp_fr10;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr11 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        CHAR(30),
    sal             bigint,
    comm            INT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr11;

DROP FOREIGN TABLE emp_fr11;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr12 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        CHAR(30),
    sal             bigint,
    comm            decimal,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr12;

DROP FOREIGN TABLE emp_fr12;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr13 (
    empno           VARCHAR2(10),
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             real,
    comm            decimal,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr13;

DROP FOREIGN TABLE emp_fr13;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr14 (
    empno           CHAR(10),
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             decimal,
    comm            decimal,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr14;

DROP FOREIGN TABLE emp_fr14;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr15 (
    empno           DATE,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             decimal,
    comm            decimal,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr15;

DROP FOREIGN TABLE emp_fr15;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr16 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             NUMBER(2),
    comm            VARCHAR2(9),
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr16;

DROP FOREIGN TABLE emp_fr16;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr17 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             VARCHAR2(9),
    comm            VARCHAR2(9),
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr17;

DROP FOREIGN TABLE emp_fr17;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr18 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             CHAR(10),
    comm            TEXT,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr18;

DROP FOREIGN TABLE emp_fr18;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr19 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             BLOB,
    comm            BLOB,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr19;

DROP FOREIGN TABLE emp_fr19;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr20 (
    empno           TEXT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        DATE,
    sal             CLOB,
    comm            CLOB,
    deptno          INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_dt_mp1');

SELECT * FROM emp_fr20;

DROP FOREIGN TABLE emp_fr20;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_fr21 (
    bol1           BOOLEAN,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_fr21;

DROP FOREIGN TABLE emp_fr21;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_fr22 (
    bol1           TEXT,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_fr22;

DROP FOREIGN TABLE emp_fr22;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_bol1 (
    bol1           INT,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol1;

DROP FOREIGN TABLE emp_bol1;


-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_bol2 (
    bol1           VARCHAR2(6),
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol2;

DROP FOREIGN TABLE emp_bol2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_bol3 (
    bol1           CHAR(6),
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol3;

DROP FOREIGN TABLE emp_bol3;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_bol4 (
    bol1             DECIMAL(4,2),
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol4;

DROP FOREIGN TABLE emp_bol4;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_bol5 (
    bol1             BLOB,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol5;

DROP FOREIGN TABLE emp_bol5;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_bol6 (
    bol1            CLOB,
    bol2           TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol6;

DROP FOREIGN TABLE emp_bol6;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_bol7 (
    bol1            CLOB,
    bol2           BOOLEAN
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol7;

DROP FOREIGN TABLE emp_bol7;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE emp_bol8 (
    bol1            DATE,
    bol2           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'bool_test');

SELECT * FROM emp_bol8;

DROP FOREIGN TABLE emp_bol8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE str_date (
    col1            DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date;

DROP FOREIGN TABLE str_date;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE str_date1 (
    col1            timestamp with time zone 
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date1;

DROP FOREIGN TABLE str_date1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE str_date2 (
    col1            TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date2;

DROP FOREIGN TABLE str_date2;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE str_date3 (
    col1           INTERVAL
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_date');

SELECT * FROM str_date3;

DROP FOREIGN TABLE str_date3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE chr_date1 (
    col1           DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date1;

DROP FOREIGN TABLE chr_date1;


-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE chr_date2 (
    col1           TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date2;

DROP FOREIGN TABLE chr_date2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE chr_date3 (
    col1           TIMESTAMP
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM chr_date3;

DROP FOREIGN TABLE chr_date3;


-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE var_date1 (
    col1           DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM var_date1;

DROP FOREIGN TABLE var_date1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE var_date2 (
    col1           TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM var_date2;

DROP FOREIGN TABLE var_date2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE var_date3 (
    col1           TIMESTAMP
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'chr_date');

SELECT * FROM var_date3;

DROP FOREIGN TABLE var_date3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE var_int (
    col1           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'var_int');

SELECT col1+1 FROM var_int;

DROP FOREIGN TABLE var_int;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE str_int (
    col1           INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'str_int');

SELECT col1+1 FROM str_int;

DROP FOREIGN TABLE str_int;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data 
(
  id         INT,
    name   text,
    sal        decimal,
    sub      text[3]   
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data;

DROP FOREIGN TABLE array_data;


-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data1 
(
  id         INT,
    name   text,
    sal        decimal,
    sub      text[]   
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data1;

DROP FOREIGN TABLE array_data1;


-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data2
(
  id         INT,
    name   text,
    sal        decimal,
    sub      text ARRAY[3]   
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data2;

DROP FOREIGN TABLE array_data2;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data3
(
  id         INT,
    name   text,
    sal        decimal,
    sub      text ARRAY    
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data3;

DROP FOREIGN TABLE array_data3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE array_data4
(
  id         INT,
    name   text,
    sal        decimal,
    sub      text    
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data4;

DROP FOREIGN TABLE array_data4;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data5
(
  id         INT,
    name   text,
    sal        decimal,
    sub      INT    
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data5;

DROP FOREIGN TABLE array_data5;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data6
(
  id         INT,
    name   text,
    sal        decimal,
    sub      DATE    
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data6;

DROP FOREIGN TABLE array_data6;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE array_data7 
(
  id         INT,
    name   text,
    sal        decimal,
    sub      INT[3]   
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data7;

DROP FOREIGN TABLE array_data7;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE array_data8 
(
  id         INT,
    name   text,
    sal        decimal,
    sub      VARCHAR2   
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'array_data');

SELECT * FROM array_data8;

DROP FOREIGN TABLE array_data8;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE emp_float1 (
    empno           SMALLINT,
    ename           VARCHAR2(10),
    job             VARCHAR2(9),
    mgr             INTEGER,
    hiredate        TIMESTAMP,
    sal             float,
    comm            DECIMAL(7,2)	,
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
    sal            double precision,
    comm            DECIMAL(7,2)	,
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

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE binary_data
(
  col1         BLOB
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data;

DROP FOREIGN TABLE binary_data;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE binary_data1
(
  col1         CLOB
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data1;

DROP FOREIGN TABLE binary_data1;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE binary_data2
(
  col1         TEXT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data2;

DROP FOREIGN TABLE binary_data2;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE binary_data3
(
  col1         VARCHAR2(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data3;

DROP FOREIGN TABLE binary_data3;

-- Create Foreign Table, data will be displayed.

CREATE FOREIGN TABLE binary_data4
(
  col1         CHAR(20)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data4;

DROP FOREIGN TABLE binary_data4;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE binary_data5
(
  col1         INT
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data5;

DROP FOREIGN TABLE binary_data5;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE binary_data6
(
  col1         DATE
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data6;

DROP FOREIGN TABLE binary_data6;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE binary_data7
(
  col1         TIME
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'binary_data');

SELECT * FROM binary_data7;

DROP FOREIGN TABLE binary_data7;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_arry (
    deptno          INT[],
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');


SELECT * FROM dept_arry;

DROP FOREIGN TABLE dept_arry;

-- Create Foreign Table, error will be displayed.

CREATE FOREIGN TABLE dept_arry1 (
    deptno          INT,
    dname           VARCHAR2[],
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

SELECT * FROM dept_arry1;

DROP FOREIGN TABLE dept_arry1;




--Cleanup

DROP EXTENSION hdfs_fdw CASCADE;

\c postgres postgres

DROP DATABASE fdw_regression;
