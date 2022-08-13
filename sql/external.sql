\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

\c contrib_regression
SET hdfs_fdw.enable_order_by_pushdown TO ON;
CREATE EXTENSION IF NOT EXISTS hdfs_fdw;
CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw
 OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);
CREATE USER MAPPING FOR public SERVER hdfs_server
 OPTIONS (username :HIVE_USER, password :HIVE_PASSWORD);

CREATE FOREIGN TABLE emp_ext (
    empno INTEGER,
    ename VARCHAR(10),
    job VARCHAR(9),
    mgr INTEGER,
    hiredate pg_catalog.DATE,
    sal INTEGER,
    comm INTEGER,
    deptno INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_ext');

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Data retrieval using SELECT statement.
SELECT * FROM emp_ext
 ORDER BY empno;

SELECT empno, ename, job, mgr, hiredate, sal, comm, deptno
 FROM emp_ext
 ORDER BY 1, 2;

SELECT * FROM emp_ext
 ORDER BY empno, deptno;

SELECT COUNT(*) FROM emp_ext;

SELECT * FROM emp_ext
 ORDER BY empno, deptno LIMIT 2;

SELECT * FROM emp_ext
 ORDER BY empno, deptno LIMIT 5 OFFSET 1;

-- Data retrieval using Group By.
SELECT deptno "Department", COUNT(emp_ext) "Total Employees"
 FROM emp_ext
 GROUP BY deptno ORDER BY deptno;

SELECT deptno, ROUND(AVG(sal)) "Average Salary", SUM(sal) "Total Salary",
 MIN(sal) "Minimum Salary", MAX(sal) "Maximum Salary" FROM emp_ext
 GROUP BY deptno
 HAVING deptno = 10 OR deptno = 20 OR deptno = 30
 ORDER BY deptno;

-- Data retrieval using Sub Queries.
SELECT * FROM emp_ext
 WHERE deptno <> ALL (SELECT deptno
 FROM dept WHERE deptno IN (10,30,40))
 ORDER BY empno, deptno;

SELECT * FROM emp_ext
 WHERE deptno NOT IN (SELECT deptno FROM dept)
 ORDER BY empno;

-- Data retrieval using UNION.
SELECT deptno, dname FROM dept
 UNION
 SELECT empno, ename FROM emp_ext
 ORDER BY deptno;

-- Data retrieval using INTERSECT.
SELECT ename FROM emp_ext WHERE empno >= 7788
 INTERSECT
 SELECT ename FROM emp_ext WHERE empno >= 7566
 ORDER BY ename;

-- Data retrieval using CROSS JOIN.
SELECT dept.dname, emp_ext.ename
 FROM dept CROSS JOIN emp_ext
 ORDER BY 1, 2;

-- Data retrieval using INNER JOIN.
SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept d, emp_ext e
 WHERE d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

-- Data retrieval using EXCEPT.
SELECT ename FROM emp_ext
 EXCEPT
 SELECT ename FROM emp_ext
 WHERE empno > 7839
 ORDER BY ename;

-- Data retrieval using OUTER JOINS.
SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept d LEFT OUTER JOIN emp_ext e
 ON d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept d RIGHT OUTER JOIN emp_ext e
 ON d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept d FULL OUTER JOIN emp_ext e
 ON d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

-- Data retrieval using CTE (With Clause).
WITH dept_count AS
 (
 SELECT deptno, COUNT(*) AS dept_count
 FROM emp_ext
 GROUP BY deptno
 )
 SELECT e.ename AS Employee_Name,
 dc.dept_count AS "Employee in Same Dept"
 FROM emp_ext e,
 dept_count dc
 WHERE e.deptno = dc.deptno
 ORDER BY e.deptno, e.ename;

WITH with_qry AS
 (
 SELECT * FROM dept
 )
 SELECT e.ename, e.sal, w.deptno, w.dname
 FROM emp_ext e, with_qry w
 WHERE e.deptno = w.deptno
 ORDER BY 1, 2, 3, 4;

-- Data retrieval using Window clause.
SELECT deptno, empno, sal, AVG(sal) OVER (PARTITION BY deptno)
 FROM emp_ext
 ORDER BY 1, 2, 3, 4;

SELECT deptno, empno, sal, COUNT(sal) OVER (PARTITION BY deptno)
 FROM emp_ext
 WHERE deptno IN (10, 30, 40, 50, 60, 70)
 ORDER BY 1, 2, 3, 4;

-- Test the EXPLAIN plans
EXPLAIN (COSTS OFF) SELECT * FROM emp_ext ORDER BY deptno;

EXPLAIN (COSTS OFF) SELECT empno,ename FROM emp_ext ORDER BY deptno;

EXPLAIN (COSTS OFF) SELECT * FROM emp_ext LIMIT 2;

EXPLAIN (COSTS OFF) SELECT * FROM emp_ext
 WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10, 30, 40))
 ORDER BY empno;

EXPLAIN (COSTS OFF)
 SELECT deptno,dname FROM dept
 UNION
 SELECT empno,ename FROM emp_ext
 ORDER BY deptno;

-- Joins with Foreign and Local Tables.
CREATE TABLE dept_lcl (
 deptno INTEGER NOT NULL CONSTRAINT dept_pk PRIMARY KEY,
 dname VARCHAR(14) CONSTRAINT dept_dname_uq UNIQUE,
 loc VARCHAR(13)
 );

INSERT INTO dept_lcl VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept_lcl VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept_lcl VALUES (30,'SALES','CHICAGO');
INSERT INTO dept_lcl VALUES (40,'OPERATIONS','BOSTON');

SELECT dept_lcl.dname, emp_ext.ename
 FROM dept_lcl CROSS JOIN emp_ext
 ORDER BY 1, 2;

SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept_lcl d, emp_ext e
 WHERE d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

SELECT d.deptno, d.dname, e.empno, e.ename, e.sal, e.deptno
 FROM dept_lcl d LEFT OUTER JOIN emp_ext e
 ON d.deptno=e.deptno
 ORDER BY 1, 2, 3, 4, 5, 6;

EXPLAIN (COSTS OFF) SELECT dept_lcl.dname, emp_ext.ename
 FROM dept_lcl CROSS JOIN emp_ext
 ORDER BY dept_lcl.deptno;

-- Cleanup
SET hdfs_fdw.enable_order_by_pushdown TO OFF;
DROP FOREIGN TABLE emp_ext;
DROP FOREIGN TABLE dept;
DROP TABLE dept_lcl;
DROP USER MAPPING FOR public SERVER hdfs_server;
DROP SERVER hdfs_server;
DROP EXTENSION hdfs_fdw;
