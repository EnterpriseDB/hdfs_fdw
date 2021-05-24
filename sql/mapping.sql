\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

\c contrib_regression

-- Test CREATE/ALTER/DROP EXTENSION commands for hdfs_fdw.
-- Create the extension hdfs_fdw using minimal syntax and it should be created
-- successfully.
CREATE EXTENSION hdfs_fdw;

-- Validate extension, server and mapping details
SELECT e.extname AS extension, s.nspname AS schema
 FROM pg_extension e
 LEFT JOIN pg_namespace s ON (e.extnamespace = s.oid)
 WHERE e.extname = 'hdfs_fdw';

-- Recreate the extension (while it already exists) and it should error out.
CREATE EXTENSION hdfs_fdw;

-- Create extension with IF NOT EXISTS syntax. It should show a notice message
-- indicating the pre-existence of extension.
CREATE EXTENSION IF NOT EXISTS hdfs_fdw;

-- DROP EXTENSION
DROP EXTENSION hdfs_fdw;

-- Attempt DROP EXTENSION with IF EXISTS clause when extension doesn't pre-exist
-- Ensure a NOTICE is raise
DROP EXTENSION IF EXISTS hdfs_fdw;

-- Create extension WITH SCHEMA to ensure extension objects are created in the schema
CREATE SCHEMA test_ext_schema;
CREATE EXTENSION IF NOT EXISTS hdfs_fdw WITH SCHEMA test_ext_schema;
SELECT e.extname AS extension, s.nspname AS schema
 FROM pg_extension e
 JOIN pg_namespace s ON (e.extnamespace = s.oid)
 WHERE e.extname = 'hdfs_fdw';

-- Change schema
ALTER EXTENSION hdfs_fdw SET SCHEMA public;
SELECT e.extname AS extension, s.nspname AS schema
 FROM pg_extension e
 JOIN pg_namespace s ON (e.extnamespace = s.oid)
 WHERE e.extname = 'hdfs_fdw';

-- Create a view to add as member to extension
CREATE FUNCTION ext_fun (a INT) RETURNS INT AS $$
 BEGIN
  RETURN 1;
 END;
$$ LANGUAGE plpgsql;

ALTER EXTENSION hdfs_fdw ADD FUNCTION ext_fun(int);

-- Should list the view in the members list
SELECT e.extname, p.proname
 FROM pg_extension e INNER JOIN pg_depend d ON (d.refobjid = e.oid)
 JOIN pg_proc p ON (p.oid = d.objid)
 WHERE e.extname = 'hdfs_fdw' ORDER BY 2;

-- Remove the view member
ALTER EXTENSION hdfs_fdw DROP FUNCTION ext_fun(int);
SELECT e.extname, p.proname
 FROM pg_extension e INNER JOIN pg_depend d ON (d.refobjid = e.oid)
 JOIN pg_proc p ON (p.oid = d.objid)
 WHERE e.extname = 'hdfs_fdw' ORDER BY 2;
DROP FUNCTION ext_fun (int);

-- CREATE SERVER

-- Create a server without providing optional parameters using the hdfs_fdw wrapper.
-- host defaults to localhost, port to 10000, client_type to hiverserver2 (RM 37660)
CREATE SERVER hdfs_srv1 FOREIGN DATA WRAPPER hdfs_fdw
 OPTIONS (client_type 'hiveserver2');
SELECT e.fdwname as "Extension", srvname AS "Server", s.srvoptions AS "Server_Options"
  FROM pg_foreign_data_wrapper e JOIN pg_foreign_server s ON e.oid = s.srvfdw
  WHERE e.fdwname = 'hdfs_fdw'
  ORDER BY 1, 2, 3;

-- Test ALTER SERVER OWNER TO, and RENAME to clauses
ALTER SERVER hdfs_srv1 RENAME TO hdfs_srv1_renamed;
SELECT e.fdwname as "Extension", srvname AS "Server", s.srvoptions AS "Server_Options"
  FROM pg_foreign_data_wrapper e JOIN pg_foreign_server s ON e.oid = s.srvfdw
  WHERE e.fdwname = 'hdfs_fdw'
  ORDER BY 1, 2, 3;
DROP SERVER hdfs_srv1_renamed;

-- Create a server providing TYPE and VERSION clauses.
-- Also check host parameter can take IP address and host a numeric port
-- Also the named parameters to have mixed cased names e.g. host, PORT, Client_Type
CREATE SERVER hdfs_srv2 TYPE 'abc' VERSION '1.0'
 FOREIGN DATA WRAPPER hdfs_fdw
 OPTIONS (host '127.0.0.1', PORT :HIVE_PORT, Client_Type 'hiveserver2');

-- Verify that the supplied clauses TYPE, VERSION and host, port, client_type are
-- as specified.
SELECT e.fdwname as "Extension", srvname AS "Server", s.srvoptions AS "Server_Options"
  FROM pg_foreign_data_wrapper e JOIN pg_foreign_server s ON e.oid = s.srvfdw
  WHERE e.fdwname = 'hdfs_fdw'
  ORDER BY 1, 2, 3;
DROP SERVER hdfs_srv2;

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
 OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);
CREATE USER MAPPING FOR public SERVER hdfs_server
 OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

CREATE FOREIGN TABLE dept (
    deptno INTEGER,
    dname VARCHAR(14),
    loc VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');
SELECT * FROM dept ORDER BY deptno;

-- Test port option
-- Negative value, raise error
ALTER SERVER hdfs_server OPTIONS (SET port '-1');
SELECT * FROM dept;
-- Zero value, raise error
ALTER SERVER hdfs_server OPTIONS (SET port '0');
SELECT * FROM dept;
-- Very large number, raise error
ALTER SERVER hdfs_server OPTIONS (SET port '12345678');
SELECT * FROM dept;
-- Empty string, raise error (RM37655)
ALTER SERVER hdfs_server OPTIONS (SET port '');
SELECT * FROM dept;
-- Non numeric value, raise error (RM37655)
ALTER SERVER hdfs_server OPTIONS (SET port 'abc');
SELECT * FROM dept;
-- Drop port to see it goes back to default 10000, should succeed
ALTER SERVER hdfs_server OPTIONS (DROP port );
SELECT * FROM dept ORDER BY deptno;

-- Test host option
-- Empty string, should fail
ALTER SERVER hdfs_server OPTIONS (SET host '');
DO
$$
BEGIN
  SELECT * FROM dept;
  EXCEPTION WHEN others THEN
	IF SQLERRM LIKE 'failed to initialize the connection: (ERROR : Could not connect to jdbc:hive2://:10000/fdw_db%' THEN
	   RAISE NOTICE 'ERROR:  failed to initialize the connection: (ERROR : Could not connect to jdbc:hive2://:10000/fdw_db within 300000 seconds)';
        ELSE
	   RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Test DROP SERVER
-- should fail, RESTRICT enforced
DROP SERVER hdfs_server RESTRICT;

-- CASCADE, should pass and drop FOREIGN TABLE and USER MAPPING
DROP SERVER hdfs_server CASCADE;

-- cleanup
DROP EXTENSION hdfs_fdw;
