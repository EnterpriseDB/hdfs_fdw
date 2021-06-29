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

SET datestyle TO ISO,DMY;

-- Retrieve data based on date comparisons.
SELECT * FROM emp
WHERE hiredate = '1980-12-17'
ORDER BY empno;

SELECT * FROM emp
WHERE hiredate < '23/01/1982'
ORDER BY empno;

SELECT * FROM emp
WHERE hiredate BETWEEN '20/02/1981' AND '23/01/1982'
ORDER BY empno;

--Cleanup
DROP FOREIGN TABLE emp;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
