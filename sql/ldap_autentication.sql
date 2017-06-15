/*---------------------------------------------------------------------------------------------------------------------
 *
 * ldap_autentication.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify the LDAP Authentication feature is working successfully.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		ldap_autentication.sql
 *
 *---------------------------------------------------------------------------------------------------------------------
 */

-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`

-- Create the database.

CREATE DATABASE fdw_regression;

\c fdw_regression postgres

-- Set the Date Style

SET datestyle TO SQL,DMY;

-- Set the Search Path to PUBLIC Schema

SET search_path TO public;

-- Create Hadoop FDW Extension.

CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server.

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE);

--**************************************************************************************************
-- To verify that error message displayed when invalid LDAP User is used
--**************************************************************************************************

-- Create postgres USER MAPPING with incorrect LDAP User.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username 'wronguser', password 'edb'); 

-- Create Foreign Tables.

CREATE FOREIGN TABLE dept_dt_mp1 
(
	deptno INTEGER, 
	dname VARCHAR(14), 
	loc VARCHAR(13) ) 
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept_dt_mp');

-- Error message will be displayed as the wrong LDAP User is mentioned in User Mapping.

SELECT * FROM dept_dt_mp1;

-- Drop user mapping and create correct user mapping.

DROP USER MAPPING FOR postgres SERVER hdfs_server;

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Data will be displayed.

SELECT * FROM dept_dt_mp1;

--**************************************************************************************************
-- To verify that error message displayed when invalid LDAP User password is used
--**************************************************************************************************

-- Drop user mapping.

DROP USER MAPPING FOR postgres SERVER hdfs_server;

-- Create postgres USER MAPPING with incorrect LDAP User password.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username 'kzeeshan', password 'wrong');

-- Error message will be displayed as the wrong LDAP User password is mentioned in User Mapping.

SELECT * FROM dept_dt_mp1;

-- Drop user mapping and create correct user mapping.

DROP USER MAPPING FOR postgres SERVER hdfs_server;

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Data will be displayed.

SELECT * FROM dept_dt_mp1;

--**************************************************************************************************
-- To verify that error message displayed when invalid LDAP User and invalid password is used
--**************************************************************************************************

-- Drop user mapping.

DROP USER MAPPING FOR postgres SERVER hdfs_server;

-- Create postgres USER MAPPING with invalid LDAP User and invalid password.

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username 'wrong', password 'wrong');

-- Error message will be displayed as the wrong LDAP User password is mentioned in User Mapping.

SELECT * FROM dept_dt_mp1;

-- Drop user mapping and create correct user mapping.

DROP USER MAPPING FOR postgres SERVER hdfs_server;

CREATE USER MAPPING FOR postgres SERVER hdfs_server OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Data will be displayed.

SELECT * FROM dept_dt_mp1;



--Cleanup

DROP FOREIGN TABLE dept_dt_mp1;

DROP EXTENSION hdfs_fdw CASCADE;


\c postgres postgres

DROP DATABASE fdw_regression;
