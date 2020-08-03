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
--                    log_remote_sql
-- Adds a new option while creating the remote server: log_remote_sql
--Default value is false. If set to true it logs the following:
-- All the SQL's sent to the remote hive server.
-- The number of times a remote scan is repeated i.e. rescanned.
-- 
--==========================================================================================

-- Create Hadoop FDW Server with wrong value in log_remote_sql 'sasaaa'.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'aasaaaa');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Error message will be displayed as wrong value in log_remote_sql 'sasaaa'. The error on CREATE SERVER will not displayed as its whats happens in PG FDW as discussed with Dev.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server with value in log_remote_sql ''.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql '', auth_type :AUTH_TYPE);

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Error message will be displayed as wrong value in log_remote_sql 'sasaaa'. The error on CREATE SERVER will not displayed as its whats happens in PG FDW as discussed with Dev.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server with value in log_remote_sql . Error message will be displayed.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql );

-- Create Hadoop FDW Server with wrong value in log_remote_sql . Error message will be displayed.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql '1');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Error message will be displayed as wrong value in log_remote_sql '1'. The error on CREATE SERVER will not displayed as its whats happens in PG FDW as discussed with Dev.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server with false value in log_remote_sql . Log will not be generated.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'false', auth_type :AUTH_TYPE);

\des+ hdfs_server

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- No Log will be generated in the log. Please note this is tested manually and cant be scripted.
--log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
--log4j:WARN Please initialize the log4j system properly.
--log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
--2017-10-19 01:43:05 EDT ERROR:  cannot drop the currently open database
--2017-10-19 01:43:05 EDT STATEMENT:  DROP DATABASE fdw_regression;

SELECT * FROM DEPT;

EXPLAIN (COSTS OFF) SELECT * FROM DEPT;

-- rescann will also not be logged.Please note this is tested manually and cant be scripted.
--log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
--log4j:WARN Please initialize the log4j system properly.
--log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.

SELECT dname, loc FROM dept d
WHERE  deptno NOT IN (SELECT deptno FROM dept
              WHERE  dept.deptno = d.deptno)
ORDER BY deptno;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server without log_remote_sql, default will be false. Log will not be generated.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);

\des+ hdfs_server

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- No Log will be generated in the log. Please note this is tested manually and cant be scripted.
--log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
--log4j:WARN Please initialize the log4j system properly.
--log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
--2017-10-19 01:43:05 EDT ERROR:  cannot drop the currently open database
--2017-10-19 01:43:05 EDT STATEMENT:  DROP DATABASE fdw_regression;

SELECT * FROM DEPT;

EXPLAIN (COSTS OFF) SELECT * FROM DEPT;

-- rescann will also not be logged.Please note this is tested manually and cant be scripted.
--log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
--log4j:WARN Please initialize the log4j system properly.
--log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.

SELECT dname, loc FROM dept d
WHERE  deptno NOT IN (SELECT deptno FROM dept
              WHERE  dept.deptno = d.deptno)
ORDER BY deptno;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server without log_remote_sql, default will be false. Log will not be generated.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE,log_remote_sql 'true', auth_type :AUTH_TYPE);

\des+ hdfs_server

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Log will be generated in the log. Please note this is tested manually and cant be scripted.
--log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
--log4j:WARN Please initialize the log4j system properly.
--log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
--2017-10-19 03:28:25 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT dname FROM fdw_db.dept] [10000]
--2017-10-19 03:28:25 EDT STATEMENT:  SELECT dname FROM DEPT;
--2017-10-19 03:28:26 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]
--2017-10-19 03:28:26 EDT STATEMENT:  SELECT dname, loc FROM dept d
--	WHERE  deptno NOT IN (SELECT deptno FROM dept
--	              WHERE  dept.deptno = d.deptno)
--	ORDER BY deptno;
--2017-10-19 03:28:26 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT deptno FROM fdw_db.dept WHERE ((deptno = ?))] [10000]
--2017-10-19 03:28:26 EDT STATEMENT:  SELECT dname, loc FROM dept d
--	WHERE  deptno NOT IN (SELECT deptno FROM dept
--	              WHERE  dept.deptno = d.deptno)
--	ORDER BY deptno;
--2017-10-19 03:28:26 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT deptno FROM fdw_db.dept WHERE ((deptno = ?))] [0]
--2017-10-19 03:28:26 EDT STATEMENT:  SELECT dname, loc FROM dept d
--	WHERE  deptno NOT IN (SELECT deptno FROM dept
--	              WHERE  dept.deptno = d.deptno)
--	ORDER BY deptno;
--2017-10-19 03:28:26 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT deptno FROM fdw_db.dept WHERE ((deptno = ?))] [1]
--2017-10-19 03:28:26 EDT STATEMENT:  SELECT dname, loc FROM dept d
--	WHERE  deptno NOT IN (SELECT deptno FROM dept
--	              WHERE  dept.deptno = d.deptno)
--	ORDER BY deptno;
--2017-10-19 03:28:26 EDT LOG:  hdfs_fdw: rescan remote SQL: [SELECT deptno FROM fdw_db.dept WHERE ((deptno = ?))] [2]
--2017-10-19 03:28:26 EDT STATEMENT:  SELECT dname, loc FROM dept d
--	WHERE  deptno NOT IN (SELECT deptno FROM dept
--	              WHERE  dept.deptno = d.deptno)
--	ORDER BY deptno;
--2017-10-19 03:28:26 EDT ERROR:  cannot drop the currently open database
--2017-10-19 03:28:26 EDT STATEMENT:  DROP DATABASE fdw_regression;

SELECT dname FROM DEPT;

EXPLAIN (COSTS OFF) SELECT * FROM DEPT;

-- rescann will also be logged.Please note this is tested manually and cant be scripted.
-- See Above Log

SELECT dname, loc FROM dept d
WHERE  deptno NOT IN (SELECT deptno FROM dept
              WHERE  dept.deptno = d.deptno)
ORDER BY deptno;





-- DROP EXTENSION
DROP EXTENSION hdfs_fdw CASCADE;
\c postgres postgres
DROP DATABASE fdw_regression;

