-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

CREATE DATABASE fdw_regression ENCODING=UTF8 LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;
\c fdw_regression postgres

CREATE EXTENSION hdfs_fdw;

--==========================================================================================
--                   use_remote_estimates
-- 4. The option use_remote_estimates was not working.
--The patch corrects the syntax error in the remote query.
--Also it was using count() to get the rows in the remote table.
--The execution of a count() on the hive table took some 15-20 seconds
--and hence enabling remote estimates was making queries slow.
--This patch changes the FDW to use EXPLAIN select * from remote_table.
--While this works only if the hive tables are analysed i.e. stats are
--updated, this technique is very fast as compared to count(*) and
--does not slow down the whole query execution.
--The minimum row count that a remote estimate can return is 1000.
-- 
--==========================================================================================

-- Use wrong parameter name, error message will be displayed.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimates 'aasaaaa');

-- Create Hadoop FDW Server with wrong value in use_remote_estimate 'sasaaa'.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate 'aasaaaa');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Error message will be displayed as wrong value in use_remote_estimate 'sasaaa'. The error on CREATE SERVER will not displayed as its whats happens in PG FDW as discussed with Dev.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server with wrong value in use_remote_estimate '1'.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate '1');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Error message will be displayed as wrong value in use_remote_estimate 'sasaaa'. The error on CREATE SERVER will not displayed as its whats happens in PG FDW as discussed with Dev.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server with wrong value in use_remote_estimate . Error message will be displayed.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate );

--************************************************************************************
-- Using false value in use_remote_estimate, 1000 rows will be returned in Query Plan
--************************************************************************************

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate 'false', log_remote_sql 'true',auth_type :AUTH_TYPE);

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

-- Rows returned by the Plan will be 1000.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Change the value to true of use_remote_estimate using ALTER SERVER Command.

ALTER SERVER hdfs_server OPTIONS (SET log_remote_sql 'false');

ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'true');

\des+ hdfs_server

-- Rows returned by the Plan will be 1000 as the table only have 4 rows and so the "The minimum row count that a remote estimate can return is 1000.".
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

--************************************************************************************
-- Using true value in use_remote_estimate, 1000 rows will be returned in Query Plan
--************************************************************************************

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate 'true', log_remote_sql 'true', fetch_size '100',auth_type :AUTH_TYPE);

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

-- Rows returned by the Plan will be 1000 as the table only have 4 rows and so the "The minimum row count that a remote estimate can return is 1000.".
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Change the value to true of use_remote_estimate using ALTER SERVER Command.

ALTER SERVER hdfs_server OPTIONS (SET log_remote_sql 'false');

ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'false');

\des+ hdfs_server

-- Rows returned by the Plan will be 1000.
SELECT * FROM DEPT;

EXPLAIN SELECT * FROM DEPT;

--Cleanup
DROP EXTENSION hdfs_fdw CASCADE;

CREATE EXTENSION hdfs_fdw;

--************************************************************************************
-- Using true value in use_remote_estimate, 1000 rows will be returned in Query Plan
--************************************************************************************

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, use_remote_estimate 'true', log_remote_sql 'true', fetch_size '100',auth_type :AUTH_TYPE);

\des+ hdfs_server

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

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

-- Total rows will be displayed.

EXPLAIN SELECT * FROM weblogs;

--Change the value to true of use_remote_estimate using ALTER SERVER Command.

ALTER SERVER hdfs_server OPTIONS (SET use_remote_estimate 'false');

\des+ hdfs_server

-- 1000 rows will be displayed.

EXPLAIN SELECT * FROM weblogs;






-- DROP EXTENSION
DROP EXTENSION hdfs_fdw CASCADE;
\c postgres postgres
DROP DATABASE fdw_regression;

