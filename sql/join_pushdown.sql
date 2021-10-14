-- Connection Settings.
\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

\c contrib_regression
CREATE EXTENSION hdfs_fdw;

-- Create Hadoop FDW Server. log_remote_sql 'true' is required to setup logging
-- for Remote SQL Sent to Hive Server.
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
  OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', auth_type :AUTH_TYPE);
CREATE SERVER hdfs_server1 FOREIGN DATA WRAPPER hdfs_fdw
  OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, log_remote_sql 'true', auth_type :AUTH_TYPE);

-- Create Hadoop USER MAPPING.
CREATE USER MAPPING FOR public SERVER hdfs_server
  OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);
CREATE USER MAPPING FOR public SERVER hdfs_server1
  OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Tables.
CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

CREATE FOREIGN TABLE dept_1 (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server1 OPTIONS (dbname 'fdw_db', table_name 'dept');


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

CREATE FOREIGN TABLE jobhist
(
    empno           INTEGER,
    startdate       pg_catalog.DATE,
    enddate         pg_catalog.DATE,
    job             VARCHAR(9),
    sal             FLOAT,
    comm            FLOAT,
    deptno          INTEGER,
    chgdesc         VARCHAR(80)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'jobhist');

CREATE FOREIGN TABLE test1
(
	c1			INTEGER,
	c2			INTEGER,
	c3			VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test1');

CREATE FOREIGN TABLE test2
(
	c1			INTEGER,
	c2			INTEGER,
	c3			VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test2');

CREATE FOREIGN TABLE test3
(
	c1			INTEGER,
	c2			INTEGER,
	c3			VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test3');

CREATE FOREIGN TABLE test4
(
	c1			INTEGER,
	c2			INTEGER,
	c3			VARCHAR(10)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test4');

SET enable_mergejoin TO off;
SET enable_hashjoin TO off;
SET enable_sort TO off;

-- Create a local table for further testing.
CREATE TABLE local_dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
);
INSERT INTO local_dept VALUES (10, 'ACCOUNTING', 'NEW YORK'), (20, 'RESEARCH', 'DALLAS');

-- Test INNER JOIN push-down
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test LEFT OUTER JOIN push-down
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e LEFT JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e.empno, e.ename, d.dname
  FROM emp e LEFT JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test RIGHT OUTER JOIN push-down
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e RIGHT JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e.empno, e.ename, d.dname
  FROM emp e RIGHT JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test FULL OUTER JOIN push-down
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e.empno, e.ename, d.dname
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.empno, t2.empno
  FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t1 FULL JOIN
    (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t2
    ON (t1.empno = t2.empno)
  ORDER BY 1, 2;
SELECT t1.empno, t2.empno
  FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t1 FULL JOIN
    (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t2
    ON (t1.empno = t2.empno)
  ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1
  FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t1 FULL JOIN
    (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t2
    ON (TRUE);
SELECT 1
  FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t1 FULL JOIN
    (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t2
    ON (TRUE)
  OFFSET 10 LIMIT 10;

-- b. one of the joining relations is a base relation and the other is a join
-- relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.empno, ss.a, ss.b
  FROM (SELECT empno FROM emp WHERE empno between 7500 and 7800) t1 FULL JOIN
    (SELECT t2.empno, t3.empno FROM emp t2 LEFT JOIN jobhist t3
      ON (t2.empno = t3.empno) WHERE (t2.empno between 7500 and 7800)) ss (a, b)
    ON (t1.empno = ss.a)
  ORDER BY t1.empno, ss.a, ss.b;
SELECT t1.empno, ss.a, ss.b
  FROM (SELECT empno FROM emp WHERE empno between 7500 and 7800) t1 FULL JOIN
    (SELECT t2.empno, t3.empno FROM emp t2 LEFT JOIN jobhist t3
      ON (t2.empno = t3.empno) WHERE (t2.empno between 7500 and 7800)) ss (a, b)
    ON (t1.empno = ss.a)
  ORDER BY t1.empno, ss.a, ss.b;

-- c. test deparsing the remote query as nested subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.empno, ss.a, ss.b
  FROM (SELECT empno FROM emp WHERE empno between 7500 and 7800) t1 FULL JOIN
    (SELECT t2.empno, t3.empno
      FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t2 FULL JOIN
       (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t3
	  ON (t2.empno = t3.empno) WHERE t2.empno IS NULL OR t2.empno IS NOT NULL) ss(a, b)
  ON (t1.empno = ss.a)
  ORDER BY t1.empno, ss.a, ss.b;
SELECT t1.empno, ss.a, ss.b
  FROM (SELECT empno FROM emp WHERE empno between 7500 and 7800) t1 FULL JOIN
    (SELECT t2.empno, t3.empno
      FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t2 FULL JOIN
       (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t3
	  ON (t2.empno = t3.empno) WHERE t2.empno IS NULL OR t2.empno IS NOT NULL) ss(a, b)
  ON (t1.empno = ss.a)
  ORDER BY t1.empno, ss.a, ss.b;

-- d. test deparsing rowmarked relations as subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.deptno, ss.a, ss.b
  FROM (SELECT deptno FROM local_dept WHERE deptno = 10) t1 INNER JOIN
    (SELECT t2.empno, t3.empno
      FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t2 FULL JOIN
       (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t3
      ON (t2.empno = t3.empno) WHERE t2.empno IS NULL OR t2.empno IS NOT NULL) ss(a, b)
  ON (TRUE)
  ORDER BY t1.deptno, ss.a, ss.b
  FOR UPDATE OF t1;
SELECT t1.deptno, ss.a, ss.b
  FROM (SELECT deptno FROM local_dept WHERE deptno = 10) t1 INNER JOIN
    (SELECT t2.empno, t3.empno
      FROM (SELECT empno FROM emp WHERE empno BETWEEN 7500 AND 7800) t2 FULL JOIN
       (SELECT empno FROM jobhist WHERE empno BETWEEN 7500 AND 7800) t3
      ON (t2.empno = t3.empno) WHERE t2.empno IS NULL OR t2.empno IS NOT NULL) ss(a, b)
  ON (TRUE)
  ORDER BY t1.deptno, ss.a, ss.b
  FOR UPDATE OF t1;

-- Test a three table join, all are foreign tables, should be pushed.
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT jh.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno) JOIN jobhist jh ON (jh.empno = e.empno)
  ORDER BY 1, 2, 3;
SELECT jh.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno) JOIN jobhist jh ON (jh.empno = e.empno)
  ORDER BY 1, 2, 3;

EXPLAIN (COSTS false, VERBOSE)
SELECT jh.empno, e.ename, d.dname
  FROM emp e, dept d, jobhist jh WHERE e.deptno = 10 AND d.deptno = 20 AND jh.deptno = 30
  ORDER BY 1, 2, 3 LIMIT 10;
SELECT jh.empno, e.ename, d.dname
  FROM emp e, dept d, jobhist jh WHERE e.deptno = 10 AND d.deptno = 20 AND jh.deptno = 30
  ORDER BY 1, 2, 3 LIMIT 10;

-- Clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno, e.ename, d.dname, d.loc
  FROM emp e LEFT JOIN (SELECT * FROM dept WHERE deptno > 20) d ON (e.deptno = d.deptno)
  WHERE (d.deptno < 50 OR d.deptno IS NULL) AND e.deptno > 20
  ORDER BY 1, 2, 3, 4;
SELECT e.empno, e.ename, d.dname, d.loc
  FROM emp e LEFT JOIN (SELECT * FROM dept WHERE deptno > 20) d ON (e.deptno = d.deptno)
  WHERE (d.deptno < 50 OR d.deptno IS NULL) AND e.deptno > 20
  ORDER BY 1, 2, 3, 4;

-- Test multiple joins in a single statement
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.ename, jh.empno, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno) RIGHT JOIN jobhist jh ON (e.empno = jh.empno)
  ORDER BY 1, 2, 3;
SELECT e.ename, jh.empno, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno) RIGHT JOIN jobhist jh ON (e.empno = jh.empno)
  ORDER BY 1, 2, 3;

-- Join two tables with FOR UPDATE clause
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno, d.deptno
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY 1, 2 FOR UPDATE OF e;
SELECT e.empno, d.deptno
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY 1, 2 FOR UPDATE OF e;

EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno, d.deptno
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY 1, 2 FOR UPDATE;
SELECT e.empno, d.deptno
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY 1, 2 FOR UPDATE;

-- Test join between a table and a resultset of a subquery.
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno, e.ename, d.dname, d.loc
  FROM emp e LEFT JOIN (SELECT * FROM dept WHERE deptno > 20) d ON (e.deptno = d.deptno)
  WHERE e.deptno > 20
  ORDER BY 1, 2, 3, 4;
SELECT e.empno, e.ename, d.dname, d.loc
  FROM emp e LEFT JOIN (SELECT * FROM dept WHERE deptno > 20) d ON (e.deptno = d.deptno)
  WHERE e.deptno > 20
  ORDER BY 1, 2, 3, 4;

-- Join with CTE
EXPLAIN (COSTS false, VERBOSE)
WITH t (c1, c2, c3) AS (
  SELECT e.empno, e.ename, d.deptno
    FROM emp e JOIN dept d ON (e.deptno = d.deptno)
) SELECT c1, c3 FROM t ORDER BY 1, 2;
WITH t (c1, c2, c3) AS (
  SELECT e.empno, e.ename, d.deptno
    FROM emp e JOIN dept d ON (e.deptno = d.deptno)
) SELECT c1, c3 FROM t ORDER BY 1, 2;

-- Test JOIN between a foreign table and a local table, should not be pushed
-- down.
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN local_dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN local_dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test whole-row reference
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e, d
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e, d
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e, d, e.ename
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;
SELECT e, d, e.ename
  FROM emp e FULL JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- SEMI JOIN, not pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno
  FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE e.deptno = d.deptno)
  ORDER BY 1;
SELECT e.empno
  FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE e.deptno = d.deptno)
  ORDER BY 1;

-- ANTI JOIN, not pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno
  FROM emp e WHERE NOT EXISTS (SELECT 1 FROM dept d WHERE e.deptno = d.deptno)
  ORDER BY 1;
SELECT e.empno
  FROM emp e WHERE NOT EXISTS (SELECT 1 FROM dept d WHERE e.deptno = d.deptno)
  ORDER BY 1;

-- CROSS JOIN can be pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT e.empno, d.deptno
  FROM emp e CROSS JOIN dept d
  ORDER BY 1, 2 LIMIT 10;
SELECT e.empno, d.deptno
  FROM emp e CROSS JOIN dept d
  ORDER BY 1, 2 LIMIT 10;

-- CROSS JOIN of two foreign tables again cross joined with a local table.
-- CROSS JOIN of foreign tables should be pushed down, and local table should
-- be joined locally.
EXPLAIN (COSTS false, VERBOSE)
SELECT e.ename, d.deptno, ld.dname
  FROM emp e CROSS JOIN dept d CROSS JOIN local_dept ld
  ORDER BY 1, 2 LIMIT 10;
SELECT e.ename, d.deptno, ld.dname
  FROM emp e CROSS JOIN dept d CROSS JOIN local_dept ld
  ORDER BY 1, 2 LIMIT 10;

-- Join two tables from two different foreign servers
EXPLAIN (COSTS false, VERBOSE)
SELECT e.deptno, d.deptno
  FROM emp e JOIN dept_1 d ON (e.deptno = d.deptno)
  ORDER BY 1, 2;
SELECT e.deptno, d.deptno
  FROM emp e JOIN dept_1 d ON (e.deptno = d.deptno)
  ORDER BY 1, 2;

-- Check join pushdown in situations where multiple userids are involved
CREATE ROLE regress_view_owner SUPERUSER;
CREATE USER MAPPING FOR regress_view_owner SERVER hdfs_server
  OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);
GRANT SELECT ON emp TO regress_view_owner;
GRANT SELECT ON dept TO regress_view_owner;
CREATE VIEW v1 AS SELECT * FROM emp;
CREATE VIEW v2 AS SELECT * FROM dept;
ALTER VIEW v2 OWNER TO regress_view_owner;

-- Can't be pushed down, different view owners
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;

-- Same user for the views, can be pushed down.
ALTER VIEW v1 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;

-- Can't be pushed down, view owner not current user
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN dept t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN dept t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;

ALTER VIEW v1 OWNER TO CURRENT_USER;
-- Can be pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN dept t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;
SELECT t1.deptno, t2.deptno
  FROM v1 t1 LEFT JOIN dept t2 ON (t1.deptno = t2.deptno)
  ORDER BY t1.deptno, t2.deptno LIMIT 10;
ALTER VIEW v1 OWNER TO regress_view_owner;

-- Non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- Unable to push {emp, dept}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, dept.deptno
  FROM (SELECT 20 FROM emp WHERE deptno = 20) q(a) RIGHT JOIN dept ON (q.a = dept.deptno)
  WHERE dept.deptno BETWEEN 10 AND 30
  ORDER BY 1, 2;
SELECT q.a, dept.deptno
  FROM (SELECT 20 FROM emp WHERE deptno = 20) q(a) RIGHT JOIN dept ON (q.a = dept.deptno)
  WHERE dept.deptno BETWEEN 10 AND 30
  ORDER BY 1, 2;

-- Ok to push {emp, dept} but not {emp, dept, jobhist}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT jobhist.deptno, q.*
  FROM jobhist LEFT JOIN
    (SELECT 13, emp.deptno, dept.deptno
       FROM emp RIGHT JOIN dept ON (emp.deptno = dept.deptno)
       WHERE emp.deptno = 10) q(a, b, c)
  ON (jobhist.deptno = q.b)
  WHERE jobhist.deptno BETWEEN 10 AND 20
  ORDER BY 1, 2;
SELECT jobhist.deptno, q.*
  FROM jobhist LEFT JOIN
    (SELECT 13, emp.deptno, dept.deptno
       FROM emp RIGHT JOIN dept ON (emp.deptno = dept.deptno)
       WHERE emp.deptno = 10) q(a, b, c)
  ON (jobhist.deptno = q.b)
  WHERE jobhist.deptno BETWEEN 10 AND 20
  ORDER BY 1, 2;

-- Test partition-wise join
SET enable_partitionwise_join TO on;

-- Create the partition table in plpgsql block as those are failing with
-- different error messages on back-branches.
-- All test cases related to partition-wise join gives an error on v96 and v95
-- as partition syntax is not supported there.
DO
$$
BEGIN
  EXECUTE 'CREATE TABLE fprt1 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c1)';
EXCEPTION WHEN others THEN
  RAISE NOTICE 'syntax error';
END;
$$
LANGUAGE plpgsql;
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (4)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (5) TO (8)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test2');

DO
$$
BEGIN
  EXECUTE 'CREATE TABLE fprt2 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c2)';
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'syntax error';
END;
$$
LANGUAGE plpgsql;
CREATE FOREIGN TABLE ftprt2_p1 PARTITION OF fprt2 FOR VALUES FROM (1) TO (4)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test3');
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (5) TO (8)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test4');

-- Inner join three tables
-- Different explain plan on v10 as partition-wise join is not supported there.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1,t2.c2,t3.c3
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t2.c2 = t3.c1)
  WHERE t1.c1 % 2 =0 ORDER BY 1,2,3;
SELECT t1.c1,t2.c2,t3.c3
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t2.c2 = t3.c1)
  WHERE t1.c1 % 2 =0 ORDER BY 1,2,3;

-- With whole-row reference; partitionwise join does not apply
-- Table alias in foreign scan is different for v12, v11 and v10.
EXPLAIN (VERBOSE, COSTS false)
SELECT t1, t2, t1.c1
  FROM fprt1 t1 JOIN fprt2 t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c3, t1.c1;
SELECT t1, t2, t1.c1
  FROM fprt1 t1 JOIN fprt2 t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c3, t1.c1;

-- Join with lateral reference
-- Different explain plan on v10 as partition-wise join is not supported there.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1,t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;
SELECT t1.c1,t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;

-- With PHVs, partitionwise join selected but no join pushdown
-- Table alias in foreign scan is different for v12, v11 and v10.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;

SET enable_partitionwise_join TO off;

-- FDW-389: Support enable_join_pushdown option at server level and table level.
-- Check only boolean values are accepted.
ALTER SERVER hdfs_server OPTIONS (ADD enable_join_pushdown 'abc11');

-- Test the option at server level.
ALTER SERVER hdfs_server OPTIONS (ADD enable_join_pushdown 'false');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

ALTER SERVER hdfs_server OPTIONS (SET enable_join_pushdown 'true');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test the option with outer rel.
ALTER FOREIGN TABLE emp OPTIONS (ADD enable_join_pushdown 'false');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

ALTER FOREIGN TABLE emp OPTIONS (SET enable_join_pushdown 'true');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test the option with inner rel.
ALTER FOREIGN TABLE dept OPTIONS (ADD enable_join_pushdown 'false');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

ALTER FOREIGN TABLE dept OPTIONS (SET enable_join_pushdown 'true');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

-- Test that setting option at table level does not affect the setting at
-- server level.
ALTER FOREIGN TABLE dept OPTIONS (SET enable_join_pushdown 'false');
ALTER FOREIGN TABLE emp OPTIONS (SET enable_join_pushdown 'false');
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT e.empno, e.ename, d.dname
  FROM emp e JOIN dept d ON (e.deptno = d.deptno)
  ORDER BY e.empno;

EXPLAIN (COSTS FALSE, VERBOSE)
SELECT t1.c1, t2.c1
  FROM test1 t1 JOIN test2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1;

-- Cleanup
DROP TABLE local_dept;
DROP OWNED BY regress_view_owner;
DROP ROLE regress_view_owner;
DROP FOREIGN TABLE emp;
DROP FOREIGN TABLE dept;
DROP FOREIGN TABLE jobhist;
DROP FOREIGN TABLE dept_1;
DROP TABLE IF EXISTS fprt1;
DROP TABLE IF EXISTS fprt2;
DROP FOREIGN TABLE test1;
DROP FOREIGN TABLE test2;
DROP FOREIGN TABLE test3;
DROP FOREIGN TABLE test4;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP USER MAPPING FOR public SERVER hdfs_server1;
DROP SERVER hdfs_server;
DROP SERVER hdfs_server1;
DROP EXTENSION hdfs_fdw;
