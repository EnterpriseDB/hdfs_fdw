-- Connection Settings.
\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
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

-- Create Hadoop USER MAPPING.
CREATE USER MAPPING FOR public SERVER hdfs_server
  OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

-- Create Foreign Table
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

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Simple aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(empno), avg(empno), min(sal), max(empno), sum(empno) * (random() <= 1)::int AS sum2 FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1, 2;
SELECT sum(empno), avg(empno), min(sal), max(empno), sum(empno) * (random() <= 1)::int AS sum2 FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1, 2;

-- Aggregate is not pushed down as aggregation contains random()
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(empno * (random() <= 1)::int) AS sum, avg(empno) FROM emp;
SELECT sum(empno * (random() <= 1)::int) AS sum, avg(empno) FROM emp;

-- Aggregate over join query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), sum(t1.empno), avg(t2.deptno) FROM  emp t1 INNER JOIN dept t2 ON (t1.deptno = t2.deptno) WHERE t1.empno = 7654;
SELECT count(*), sum(t1.empno), avg(t2.deptno) FROM  emp t1 INNER JOIN dept t2 ON (t1.deptno = t2.deptno) WHERE t1.empno = 7654;

-- Not pushed down due to local conditions present in underneath input rel
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.deptno), count(t2.deptno) FROM emp t1 INNER JOIN dept t2 ON (t1.deptno = t2.deptno) WHERE ((t1.deptno * t2.deptno)/(t1.deptno * t2.deptno)) * random() <= 1;
SELECT sum(t1.deptno), count(t2.deptno) FROM emp t1 INNER JOIN dept t2 ON (t1.deptno = t2.deptno) WHERE ((t1.deptno * t2.deptno)/(t1.deptno * t2.deptno)) * random() <= 1;

-- GROUP BY clause HAVING expressions
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal+20, sum(sal) * (sal+20) FROM emp GROUP BY sal+20 ORDER BY sal+20;

-- Aggregates in subquery are pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(x.a), sum(x.a) FROM (SELECT sal a, sum(sal) b FROM emp GROUP BY sal ORDER BY 1, 2) x;
SELECT count(x.a), sum(x.a) FROM (SELECT sal a, sum(sal) b FROM emp GROUP BY sal ORDER BY 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal * (random() <= 1)::int AS sum1, sum(empno) * sal AS sum2 FROM emp GROUP BY sal ORDER BY 1, 2;
SELECT sal * (random() <= 1)::int AS sum1, sum(empno) * sal AS sum2 FROM emp GROUP BY sal ORDER BY 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal * (random() <= 1)::int AS c2 FROM emp GROUP BY sal * (random() <= 1)::int ORDER BY 1;
SELECT sal * (random() <= 1)::int AS c2 FROM emp GROUP BY sal * (random() <= 1)::int ORDER BY 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(sal) w, sal x, 5 y, 7.0 z FROM emp GROUP BY 2, y, 9.0::int ORDER BY 2;
SELECT count(sal) w, sal x, 5 y, 7.0 z FROM emp GROUP BY 2, y, 9.0::int ORDER BY 2;

-- Testing HAVING clause shippability
SET enable_sort TO ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, sum(empno) FROM emp GROUP BY sal HAVING avg(empno) > 500 and sum(empno) > 4980 ORDER BY sal;
SELECT sal, sum(empno) FROM emp GROUP BY sal HAVING avg(empno) > 500 and sum(empno) > 4980 ORDER BY sal;

-- Using expressions in HAVING clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ename, count(empno) FROM emp GROUP BY ename HAVING sqrt(max(empno)) = sqrt(7900) ORDER BY 1, 2;
SELECT ename, count(empno) FROM emp GROUP BY ename HAVING sqrt(max(empno)) = sqrt(7900) ORDER BY 1, 2;

SET enable_sort TO off;
-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM (SELECT ename, count(empno) FROM emp GROUP BY ename HAVING (avg(empno) / avg(empno)) * random() <= 1 and avg(empno) < 500) x;
SELECT count(*) FROM (SELECT ename, count(empno) FROM emp GROUP BY ename HAVING (avg(empno) / avg(empno)) * random() <= 1 and avg(empno) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal) FROM emp GROUP BY empno HAVING avg(sal * (random() <= 1)::int) > 1 ORDER BY 1;
SELECT sum(sal) FROM emp GROUP BY empno HAVING avg(sal * (random() <= 1)::int) > 1 ORDER BY 1;

-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates
-- ORDER BY within aggregates (same column used to order) are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal ORDER BY sal) FROM emp WHERE sal < 1000 GROUP BY empno ORDER BY 1;
SELECT sum(sal ORDER BY sal) FROM emp WHERE sal < 1000 GROUP BY empno ORDER BY 1;

-- ORDER BY within aggregate (different column used to order also using DESC)
-- are not pushed.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal ORDER BY empno desc) FROM emp WHERE empno > 5000 and sal > 1000;
SELECT sum(sal ORDER BY empno desc) FROM emp WHERE empno > 5000 and sal > 1000;

-- DISTINCT within aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (sal)) FROM emp WHERE empno = 7654  and sal < 2000;
SELECT sum(DISTINCT (sal)) FROM emp WHERE empno = 7654  and sal < 2000;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (t1.sal)) FROM emp t1 join dept t2 ON (t1.deptno = t2.deptno) WHERE t1.empno < 8000 or (t1.empno is null and t2.deptno < 30) GROUP BY (t2.deptno) ORDER BY 1;
SELECT sum(DISTINCT (t1.sal)) FROM emp t1 join dept t2 ON (t1.deptno = t2.deptno) WHERE t1.empno < 8000 or (t1.empno is null and t2.deptno < 30) GROUP BY (t2.deptno) ORDER BY 1;

-- DISTINCT with aggregate within aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT sal) FROM emp WHERE empno = 7654 and sal < 2000;
SELECT sum(DISTINCT sal) FROM emp WHERE empno = 7654 and sal < 2000;

-- DISTINCT, ORDER BY and FILTER within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal%3), sum(DISTINCT sal%3 ORDER BY sal%3) filter (WHERE sal%3 < 2), empno FROM emp WHERE empno = 7654 GROUP BY empno;
SELECT sum(sal%3), sum(DISTINCT sal%3 ORDER BY sal%3) filter (WHERE sal%3 < 2), empno FROM emp WHERE empno = 7654 GROUP BY empno;

-- FILTER within aggregate, not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal) filter (WHERE sal < 2000 and empno > 1000) FROM emp GROUP BY empno ORDER BY 1 nulls last;
SELECT sum(sal) filter (WHERE sal < 2000 and empno > 1000) FROM emp GROUP BY empno ORDER BY 1 nulls last;

-- Outer query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.deptno = 20 and t1.empno < 8000) FROM emp t1 WHERE t1.empno = 7654) FROM dept t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.deptno = 20 and t1.empno < 8000) FROM emp t1 WHERE t1.empno = 7654) FROM dept t2 ORDER BY 1;

-- Inner query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(t1.empno) filter (WHERE t2.deptno = 20) FROM emp t1 WHERE t1.empno = 7654) FROM dept t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(t1.empno) filter (WHERE t2.deptno = 20) FROM emp t1 WHERE t1.empno = 7654) FROM dept t2 ORDER BY 1;

-- Ordered-sets within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, rank('10'::varchar) within group (ORDER BY ename), percentile_cont(sal/5000::numeric) within group (ORDER BY empno) FROM emp GROUP BY sal HAVING percentile_cont(sal/200::numeric) within group (ORDER BY empno) < 8000 ORDER BY sal;
SELECT sal, rank('10'::varchar) within group (ORDER BY ename), percentile_cont(sal/5000::numeric) within group (ORDER BY empno) FROM emp GROUP BY sal HAVING percentile_cont(sal/5000::numeric) within group (ORDER BY empno) < 8000 ORDER BY sal;

-- Using multiple arguments within aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT empno, rank(empno, sal) within group (ORDER BY empno, sal) FROM emp GROUP BY empno, sal HAVING empno = 7654 ORDER BY 1;
SELECT empno, rank(empno, sal) within group (ORDER BY empno, sal) FROM emp GROUP BY empno, sal HAVING empno = 7654 ORDER BY 1;

-- Subquery in FROM clause HAVING aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), x.b FROM emp, (SELECT deptno a, sum(deptno) b FROM dept GROUP BY deptno) x WHERE emp.deptno = x.a GROUP BY x.b ORDER BY 1, 2;
SELECT count(*), x.b FROM emp, (SELECT deptno a, sum(deptno) b FROM dept GROUP BY deptno) x WHERE emp.deptno = x.a GROUP BY x.b ORDER BY 1, 2;

-- Join with IS NULL check in HAVING
EXPLAIN (VERBOSE, COSTS OFF)
SELECT avg(t1.empno), sum(t2.deptno) FROM emp t1 join dept t2 ON (t1.deptno = t2.deptno) GROUP BY t2.deptno HAVING (avg(t1.empno) is null and sum(t2.deptno) > 10) or sum(t2.deptno) is null ORDER BY 1 nulls last, 2;
SELECT avg(t1.empno), sum(t2.deptno) FROM emp t1 join dept t2 ON (t1.deptno = t2.deptno) GROUP BY t2.deptno HAVING (avg(t1.empno) is null and sum(t2.deptno) > 10) or sum(t2.deptno) is null ORDER BY 1 nulls last, 2;

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(sal) * (random() <= 1)::int AS sum FROM emp ORDER BY 1;
SELECT sum(sal) * (random() <= 1)::int AS sum FROM emp ORDER BY 1;

-- Check with placeHolderVars
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.b, count(emp.deptno), sum(q.a) FROM emp left join (SELECT min(13), avg(emp.deptno), sum(dept.deptno) FROM emp right join dept ON (emp.deptno = dept.deptno) WHERE emp.deptno = 20) q(a, b, c) ON (emp.deptno = q.b) WHERE emp.deptno between 10 and 30 GROUP BY q.b ORDER BY 1 nulls last, 2;
SELECT q.b, count(emp.deptno), sum(q.a) FROM emp left join (SELECT min(13), avg(emp.deptno), sum(dept.deptno) FROM emp right join dept ON (emp.deptno = dept.deptno) WHERE emp.deptno = 20) q(a, b, c) ON (emp.deptno = q.b) WHERE emp.deptno between 10 and 30 GROUP BY q.b ORDER BY 1 nulls last, 2;

-- Not supported cases
-- Grouping sets
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, sum(empno) FROM emp WHERE sal > 1000 GROUP BY rollup(sal) ORDER BY 1 nulls last;
SELECT sal, sum(empno) FROM emp WHERE sal > 1000 GROUP BY rollup(sal) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, sum(empno) FROM emp WHERE sal > 1000 GROUP BY cube(sal) ORDER BY 1 nulls last;
SELECT sal, sum(empno) FROM emp WHERE sal > 1000 GROUP BY cube(sal) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, ename, sum(empno) FROM emp WHERE sal > 1000 GROUP BY grouping sets(sal, ename) ORDER BY 1 nulls last, 2 nulls last;
SELECT sal, ename, sum(empno) FROM emp WHERE sal > 1000 GROUP BY grouping sets(sal, ename) ORDER BY 1 nulls last, 2 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, sum(empno), grouping(sal) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1 nulls last;
SELECT sal, sum(empno), grouping(sal) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT sum(empno) s FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;
SELECT DISTINCT sum(empno) s FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;

-- WindowAgg
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, sum(sal), count(sal) over (partition by sal) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;
SELECT sal, sum(sal), count(sal) over (partition by sal) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, array_agg(sal) over (partition by sal ORDER BY sal desc) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;
SELECT sal, array_agg(sal) over (partition by sal ORDER BY sal desc) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, array_agg(sal) over (partition by sal ORDER BY sal range between current row and unbounded following) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;
SELECT sal, array_agg(sal) over (partition by sal ORDER BY sal range between current row and unbounded following) FROM emp WHERE sal > 1000 GROUP BY sal ORDER BY 1;

-- User defined function for user defined aggregate, VARIADIC
CREATE FUNCTION least_accum(anyelement, variadic anyarray)
returns anyelement language sql AS
  'SELECT least($1, min($2[i])) FROM generate_subscripts($2,2) g(i)';
CREATE aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
-- Not pushed down due to user defined aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sal, least_agg(empno) FROM emp GROUP BY sal ORDER BY sal;
SELECT sal, least_agg(empno) FROM emp GROUP BY sal ORDER BY sal;

-- Test partition-wise aggregates
SET enable_partitionwise_aggregate TO on;

-- Create the partition tables.
CREATE TABLE fprt1 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c1);
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (4)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (5) TO (8)
  SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'test2');

-- Plan with partitionwise aggregates is enabled
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 2;
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, avg(c1), max(c1), count(*) FROM fprt1 GROUP BY c2 HAVING sum(c1) < 700 ORDER BY 1;
SELECT c2, avg(c1), max(c1), count(*) FROM fprt1 GROUP BY c2 HAVING sum(c1) < 700 ORDER BY 1;

SET enable_partitionwise_aggregate TO off;

-- Cleanup
DROP aggregate least_agg(variadic items anyarray);
DROP FUNCTION least_accum(anyelement, variadic anyarray);
DROP FOREIGN TABLE emp;
DROP FOREIGN TABLE dept;
DROP FOREIGN TABLE ftprt1_p1;
DROP FOREIGN TABLE ftprt1_p2;
DROP TABLE IF EXISTS fprt1;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
