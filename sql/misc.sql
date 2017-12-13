/*---------------------------------------------------------------------------------------------------------------------
 *
 * mapping.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify the misc scenarios.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		misc.sql
 *
 *---------------------------------------------------------------------------------------------------------------------
 */

-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`

CREATE DATABASE fdw_regression ENCODING=UTF8 LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;
\c fdw_regression postgres

CREATE EXTENSION hdfs_fdw;

--==========================================================================================
--                    DESC Table Issue
-- In each query execution FDW used to issue a DESC table_name
-- statement to the hive server. This patch removes the need
-- for issuing this command and hence removes all the code
-- associated with it.
-- Previously the FDW needed column type while retrieving
-- its value. There is no need of column type any more.
-- Hence the need of this round trip is eliminated.
-- This enhances the performance as well cleans up the code.
--==========================================================================================

-- Create Hadoop FDW Server. log_remote_sql 'true' is required to setup logging for Remote SQL Sent to Hive Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Now the DESC command and column type are not sent to remote server, this information is now not displayed in Serer log as below example is shown as this cant be scripted hence it is tested manually as per discussion with dev Team.

-- Following is sent in log
--2017-10-13 03:40:58 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]
SELECT * FROM DEPT;

-- Following is sent in log
-- 2017-10-13 03:40:59 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept WHERE ((deptno = '20'))] [10000]
SELECT * FROM DEPT WHERE DEPTNO = 20;

-- Following is sent in log
--2017-10-13 03:44:39 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT deptno FROM fdw_db.dept WHERE ((deptno >= '20')) AND ((deptno <= '30'))] [10000]
SELECT deptno FROM DEPT WHERE DEPTNO BETWEEN 20 AND 30;


--==========================================================================================
--                          Correct Query Costs

--The costing system of FDW was not correct and query costs were
--coming up as negative values. The patch corrects the costing
--mechanism. The patch increases the ForeignScan node costs
--to make sure that the planner inserts materialize nodes at
--appropriate places in the plan tree to avoid UN-necessary
--rescans. This is a major improvement and makes most of the
--TPC-H queries execute in a reasonable time frame.
--==========================================================================================

--Now the cost is not negitive.
EXPLAIN SELECT * FROM DEPT;
EXPLAIN SELECT * FROM DEPT WHERE DEPTNO = 20;
EXPLAIN SELECT deptno FROM DEPT WHERE DEPTNO BETWEEN 20 AND 30;

--==========================================================================================
--                         fetch_size
-- Adds a new option while creating the remote server: fetch_size
-- Default value is 10,000. For example:
-- CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
-- OPTIONS (host '54.90.191.116',
-- use_remote_estimate 'true',
-- fetch_size '100000');
-- The user specified value is used as parameter to the JDBC API
-- setFetchSize(fetch_size)

--
--==========================================================================================

--Verify that defualt fetch size is 10,000. This vaule will be sent to server log.
--This cant be manaully verified so following log is attached as example where the default vaule 10000 is sent to server.
-- CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host 'localhost', port '10000', client_type 'spark', log_remote_sql 'true');

-- 2017-10-13 03:51:17 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]      here [10000 is default size]
SELECT * FROM DEPT;

-- Verify the default vaule from Server Descritpion.
-- The default vaule will not be displayed and as per the discussion with Dev its the expected behavour.
\des+ hdfs_server

-- Change the vaule of fetch size.
ALTER SERVER hdfs_server OPTIONS (SET fetch_size '200');

-- Verify that the changed vaule is reflected.
\des+ hdfs_server

-- Drop Server to create new one to test option fetch_size.
DROP SERVER hdfs_server CASCADE;

-- Create foreign server with option fetch_size
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', fetch_size '200');

-- Create Hadoop USER MAPPING.
CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Verify the vaule of fetch_size
\des+ hdfs_server

--Verify that user set fetch size is 200. This vaule will be sent to server log.
--This cant be manaully verified so following log is attached as example.

-- 10-13 04:53:45 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [200]
SELECT * FROM DEPT;

--Reset fetch_size to the default vaule
ALTER SERVER hdfs_server OPTIONS (DROP fetch_size );

--Verify that fetch size is set back to default vaule of 10000. This vaule will be sent to server log.
--This cant be manaully verified so following log is attached as example where the default vaule 10000 is sent to server.

-- 2017-10-13 04:58:26 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT * FROM fdw_db.dept] [10000]
SELECT * FROM DEPT;

-- Verify the vaule of fetch_size, the vaule fetch size is not visisble and its as designed.
\des+ hdfs_server

-- To check vaule less then rows in Table with fetch_size.
-- Drop Server to create new one to test option fetch_size.
DROP SERVER hdfs_server CASCADE;

-- Create foreign server with option fetch_size
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', fetch_size '1');

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- All the data from the table will be displayed as the fetch_size is not related with # of rows as discussed with dev.
-- As per the discussion fetch_size will not have any impact on EXPLAIN PLANS
SELECT deptno FROM DEPT;

-- To check with a large vaule fetch_size.
-- Drop Server to create new one to test option fetch_size.
DROP SERVER hdfs_server CASCADE;

-- Create foreign server with option fetch_size
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', fetch_size '700000');

-- Create Hadoop USER MAPPING.
CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- 2017-10-13 05:48:14 EDT LOG:  hdfs_fdw: prepare remote SQL: [SELECT deptno FROM fdw_db.dept] [700000]
SELECT deptno FROM DEPT;

-- To check with an empty vaule fetch_size.
-- Drop Server to create new one to test option fetch_size.
DROP SERVER hdfs_server CASCADE;

-- Create foreign server with option fetch_size without vaule, error message will be displayed.
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', fetch_size );







-- DROP EXTENSION
DROP EXTENSION hdfs_fdw CASCADE;
\c postgres postgres
DROP DATABASE fdw_regression;

