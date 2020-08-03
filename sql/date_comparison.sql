-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

-- Create the database.

CREATE DATABASE fdw_regression;

\c fdw_regression postgres

-- Set the Date Style

SET datestyle TO ISO,DMY;

-- Set the Search Path to PUBLIC Schema

SET search_path TO public;

-- Create Hadoop FDW Extension.

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

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

SELECT * FROM emp;

-- Retive data based on date comparisons.

SELECT * FROM emp
WHERE hiredate = '1980-12-17';

SELECT * FROM emp
WHERE hiredate < '23/01/1982';

SELECT * FROM emp
WHERE hiredate BETWEEN '20/02/1981' AND '23/01/1982';



--Cleanup

DROP EXTENSION hdfs_fdw CASCADE;

\c postgres postgres

DROP DATABASE fdw_regression;
