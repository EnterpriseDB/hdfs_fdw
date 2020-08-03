-- Connection Settings.

\set HIVE_SERVER         `echo \'"$HIVE_SERVER"\'`
\set HIVE_CLIENT_TYPE    `echo \'"$CLIENT_TYPE"\'`
\set HIVE_PORT           `echo \'"$HIVE_PORT"\'`
\set HIVE_USER           `echo \'"$HIVE_USER"\'`
\set HIVE_PASSWORD       `echo \'"$HIVE_PASSWORD"\'`
\set AUTH_TYPE           `echo \'"$AUTH_TYPE"\'`

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

CREATE SERVER hdfs_server FOREIGN DATA WRAPPER hdfs_fdw OPTIONS(host :HIVE_SERVER, port :HIVE_PORT, client_type :HIVE_CLIENT_TYPE, auth_type :AUTH_TYPE);

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

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

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

CREATE FOREIGN TABLE jobhist 
(
    empno           INTEGER,
    startdate       DATE,
    enddate         DATE,
    job             VARCHAR(9),
    sal             INTEGER,
    comm            INTEGER,
    deptno          INTEGER,
    chgdesc         VARCHAR(80)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'jobhist');

-- Retrive Data from Foreign Table using SELECT Statement.

SELECT * FROM DEPT;

SELECT deptno,dname,loc FROM DEPT;

SELECT * FROM EMP;

SELECT * FROM EMP ORDER BY deptno;

SELECT * FROM EMP ORDER BY deptno, empno;

SELECT * FROM EMP ORDER BY deptno DESC, empno;

SELECT * FROM EMP ORDER BY deptno DESC, empno DESC;

SELECT * FROM DEPT ORDER BY dname DESC;

SELECT DISTINCT deptno FROM EMP ORDER BY deptno;

SELECT DISTINCT mgr, deptno FROM EMP ORDER BY mgr,deptno;

SELECT DISTINCT empno, deptno FROM EMP ORDER BY empno, deptno;

SELECT deptno, dname, loc FROM dept;

SELECT deptno, dname  FROM dept;

SELECT ename as "Employee Name" FROM emp;

SELECT deptno, sal, comm FROM Emp ORDER BY deptno;

SELECT COUNT(*) FROM jobhist;

SELECT COUNT(DISTINCT deptno) FROM jobhist;

SELECT COUNT(empno) FROM jobhist;

SELECT empno,startdate,job,sal,deptno FROM jobhist LIMIT 2;

SELECT * FROM emp ORDER BY empno LIMIT ALL;

SELECT * FROM emp ORDER BY empno LIMIT NULL;

SELECT * FROM emp ORDER BY emp OFFSET 13;

SELECT * FROM emp ORDER BY emp LIMIT 5 OFFSET 1;

-- Retrive Data from Foreign Table using Group By Clause.

SELECT deptno "Department", COUNT(emp) "Total Employees" FROM emp GROUP BY deptno ORDER BY deptno;

SELECT deptno, SUM(sal) FROM emp GROUP BY deptno HAVING deptno IN (10, 30) ORDER BY deptno;

SELECT deptno, SUM(sal) FROM emp
GROUP BY deptno
HAVING SUM(sal) > 9400
ORDER BY deptno;

SELECT deptno, ROUND(AVG(sal)) FROM emp
GROUP BY deptno
HAVING deptno IN (10,20,40)
ORDER BY deptno DESC;

SELECT deptno, ROUND(AVG(sal)) FROM emp
GROUP BY deptno
HAVING deptno NOT IN (10,20,40)
ORDER BY deptno DESC;

SELECT deptno, ROUND(AVG(sal)) "Average Salary", SUM(sal) "Total Salary", MIN(sal) "Minimum Salary", MAX(sal) "Maximum Salary" FROM emp
GROUP BY deptno
HAVING deptno = 10 OR deptno = 20 OR deptno = 30
ORDER BY deptno;

-- Retrive Data from Foreign Table using Sub Queries.

SELECT * FROM emp
WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno;

SELECT * FROM dept
WHERE EXISTS
	(SELECT * FROM emp
	WHERE dept.deptno = emp.deptno)
ORDER BY deptno;

SELECT * FROM emp
WHERE deptno NOT IN (SELECT deptno FROM dept)
ORDER BY empno;

SELECT * FROM emp
WHERE deptno = ANY (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno;

SELECT * FROM dept
WHERE NOT EXISTS
(SELECT * FROM emp
WHERE dept.deptno = emp.deptno)
ORDER BY deptno;

SELECT * FROM emp
WHERE empno IN (SELECT empno FROM emp WHERE sal BETWEEN 5000 AND 6000)
ORDER BY empno;

-- Retrive Data from Foreign Table using UNION Operator.

SELECT deptno,dname FROM dept 
UNION
SELECT empno,ename FROM emp
ORDER BY deptno;

SELECT dname FROM dept 
UNION
SELECT ename FROM emp
ORDER BY dname;

SELECT dname FROM dept 
UNION ALL
SELECT ename FROM emp
ORDER BY dname;

-- Retrive Data from Foreign Table using INTERSECT Operator.

SELECT ename FROM emp WHERE empno >= 7788 
INTERSECT
SELECT ename FROM emp WHERE empno >= 7566
ORDER BY ename;

SELECT ename FROM emp WHERE empno >= 7788 
INTERSECT ALL
SELECT ename FROM emp WHERE empno >= 7566
ORDER BY ename;

-- Retrive Data from Foreign Table using CROSS JOIN.

SELECT dept.dname, emp.ename FROM dept CROSS JOIN emp
ORDER BY dept.deptno;

-- Retrive Data from Foreign Table using INNER JOIN.

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d, emp e
WHERE d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d INNER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno;

-- Retrive Data from Foreign Table using EXCEPT.

SELECT ename FROM emp 
EXCEPT
SELECT ename FROM emp WHERE empno > 7839
ORDER BY ename;

SELECT ename FROM emp 
EXCEPT ALL
SELECT ename FROM emp WHERE empno > 7839
ORDER BY ename;

-- Retrive Data from Foreign Table using OUTER JOINS.

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d LEFT OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d RIGHT OUTER JOIN emp e ON d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d FULL OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno,e.empno,e.sal;

-- Retrive Data from Foreign Table using CTE (With Clause).

WITH dept_count AS 
(
  SELECT deptno, COUNT(*) AS dept_count
  FROM   emp
  GROUP BY deptno
)
SELECT e.ename AS Employee_Name,
       dc.dept_count AS "Employee in Same Dept"
FROM   emp e,
       dept_count dc
WHERE  e.deptno = dc.deptno
ORDER BY e.deptno, e.ename;

WITH with_qry AS 
(
SELECT * FROM dept
)
SELECT e.ename, e.sal, w.deptno, w.dname
FROM emp e, with_qry w
WHERE e.deptno = w.deptno
ORDER BY e.deptno, e.ename;

WITH dept_costs AS 
(
SELECT dname, SUM(sal) dept_total
FROM   emp e, dept d
WHERE  e.deptno = d.deptno
GROUP BY dname
),
avg_cost AS
(
SELECT SUM(dept_total)/COUNT(*) avg
FROM   dept_costs
)
SELECT * FROM dept_costs
WHERE  dept_total > (SELECT avg FROM avg_cost)
ORDER BY dname;

WITH pol_data AS 
(
SELECT *
FROM   dept
WHERE deptno = 70)
SELECT * FROM pol_data;

-- Retrive Data from Foreign Table using Window Clause.

SELECT deptno, empno, sal, AVG(sal) OVER (PARTITION BY deptno) 
FROM emp
ORDER BY deptno, empno;

SELECT deptno, empno, sal, COUNT(sal) OVER (PARTITION BY deptno) 
FROM emp
WHERE deptno IN (10,30,40,50,60,70)
ORDER BY deptno, empno;

SELECT deptno, empno, sal, SUM(sal) OVER (PARTITION BY deptno) 
FROM emp
ORDER BY deptno, empno;

SELECT deptno, empno, sal, RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) FROM emp;

SELECT deptno, empno, sal, DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) FROM emp
WHERE deptno IN (10,30,40,50,60,70);

SELECT deptno, empno, sal , MIN(sal) OVER (PARTITION BY deptno),
MAX(sal) OVER (PARTITION BY deptno)
FROM emp
WHERE deptno IN (10,30,50,70)
ORDER BY deptno, empno;

SELECT deptno, empno, sal , MIN(sal) OVER win,
MAX(sal) OVER win, SUM(sal) OVER win,AVG(sal) OVER win
FROM emp
WHERE deptno IN (10,30,50,70)
WINDOW win AS (PARTITION BY deptno)
ORDER BY deptno, empno;

SELECT deptno, empno, sal, ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal DESC) FROM emp;

SELECT deptno, empno, sal, FIRST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal DESC),LAST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal DESC) 
FROM emp;

SELECT deptno, empno, sal,
LEAD(sal, 1, 0) OVER (PARTITION BY deptno ORDER BY sal DESC) NEXT_LOWER_SAL,
LAG(sal, 1, 0) OVER (PARTITION BY deptno ORDER BY sal DESC ) PREV_HIGHER_SAL
FROM emp
WHERE deptno IN (10, 20,30,40,50)
ORDER BY deptno, sal DESC;

SELECT deptno, empno, sal, PERCENT_RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) FROM emp;

SELECT deptno, empno, sal, NTH_VALUE(sal,2) 
OVER (PARTITION BY deptno ORDER BY sal) FROM emp;

-- Retrive Data from Foreign Table using Query Optimizer.

EXPLAIN (COSTS OFF) SELECT * FROM DEPT;

EXPLAIN (COSTS OFF) SELECT deptno,dname,loc FROM DEPT;

EXPLAIN (COSTS OFF) SELECT * FROM EMP ORDER BY deptno;

EXPLAIN (COSTS OFF) SELECT empno,ename FROM EMP ORDER BY deptno;

EXPLAIN (COSTS OFF) SELECT empno,ename FROM EMP;

EXPLAIN (COSTS OFF) SELECT DISTINCT deptno FROM EMP;

EXPLAIN (COSTS OFF) SELECT DISTINCT mgr, deptno FROM EMP;

EXPLAIN (COSTS OFF) SELECT ename as "Employee Name" FROM emp;

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM jobhist;

EXPLAIN (COSTS OFF) SELECT * FROM jobhist LIMIT 2;

EXPLAIN (COSTS OFF) SELECT * FROM emp ORDER BY emp LIMIT 5 OFFSET 1;

EXPLAIN (COSTS OFF) SELECT deptno "Department", SUM(sal) "Total Salary…." FROM emp
GROUP BY deptno
HAVING SUM(sal) > 8750
ORDER BY deptno;

EXPLAIN (COSTS OFF) SELECT * FROM emp
WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno;

EXPLAIN (COSTS OFF) 
SELECT deptno,dname FROM dept 
UNION
SELECT empno,ename FROM emp
ORDER BY deptno;

EXPLAIN (COSTS OFF)
SELECT ename FROM emp WHERE empno >= 7788 
INTERSECT
SELECT ename FROM emp WHERE empno >= 7566
ORDER BY ename;

EXPLAIN (COSTS OFF)
SELECT dept.dname, emp.ename FROM dept CROSS JOIN emp
ORDER BY dept.deptno;

EXPLAIN (COSTS OFF)
SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d INNER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno;

EXPLAIN (COSTS OFF)
SELECT ename FROM emp 
EXCEPT
SELECT ename FROM emp WHERE empno > 7839
ORDER BY ename;

EXPLAIN (COSTS OFF)
SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d LEFT OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno;

EXPLAIN (COSTS OFF)
WITH dept_count AS 
(
SELECT deptno, COUNT(*) AS dept_count
FROM emp
GROUP BY deptno
)
SELECT e.ename AS Employee_Name,
dc.dept_count AS "Employee in Same Dept"
FROM emp e,
dept_count dc
WHERE e.deptno = dc.deptno
ORDER BY e.deptno, e.ename;

EXPLAIN (COSTS OFF)
SELECT deptno, empno, sal, AVG(sal) OVER (PARTITION BY deptno) 
FROM emp
ORDER BY deptno, empno;

-- Retrive Data from Foreign Table using Query Optimizer.

CREATE TABLE dept_lcl (
deptno INTEGER NOT NULL CONSTRAINT dept_pk PRIMARY KEY,
dname VARCHAR(14) CONSTRAINT dept_dname_uq UNIQUE,
loc VARCHAR(13)
);

INSERT INTO dept_lcl VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept_lcl VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept_lcl VALUES (30,'SALES','CHICAGO');
INSERT INTO dept_lcl VALUES (40,'OPERATIONS','BOSTON');

SELECT dept_lcl.dname, emp.ename FROM dept_lcl CROSS JOIN emp
ORDER BY dept_lcl.deptno;

EXPLAIN (COSTS OFF) SELECT dept_lcl.dname, emp.ename FROM dept_lcl CROSS JOIN emp
ORDER BY dept_lcl.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d, emp e
WHERE d.deptno=e.deptno
ORDER BY d.deptno,e.empno,e.sal;

EXPLAIN (COSTS OFF) SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d, emp e
WHERE d.deptno=e.deptno
ORDER BY d.deptno,e.empno,e.sal;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d LEFT OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno,e.empno,e.sal;

EXPLAIN (COSTS OFF) SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d LEFT OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno,e.empno,e.sal;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d RIGHT OUTER JOIN emp e ON d.deptno=e.deptno
ORDER BY d.deptno;

EXPLAIN (COSTS OFF) SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d RIGHT OUTER JOIN emp e ON d.deptno=e.deptno ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d FULL OUTER JOIN emp e
ON d.deptno=e.deptno
ORDER BY d.deptno;

EXPLAIN (COSTS OFF) SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d FULL OUTER JOIN emp e
ON d.deptno=e.deptno ORDER BY d.deptno;

--VACUUM, ANAYLZE

VACUUM emp;   

VACUUM FULL emp;

VACUUM FREEZE emp;

VACUUM;

ANALYZE jobhist;

ANALYZE emp(empno); 

VACUUM ANALYZE emp;

--Views

CREATE VIEW smpl_vw
AS
SELECT * FROM emp
ORDER BY empno;

SELECT * FROM smpl_vw;

CREATE VIEW comp_vw (empno,ename,job,sal,comm,deptno,dname)
AS
SELECT s.empno,s.ename,s.job,s.sal,s.comm,d.deptno,d.dname FROM dept d, emp s
WHERE d.deptno=s.deptno
AND d.deptno = 10
ORDER BY s.empno;

SELECT * FROM comp_vw;

CREATE OR REPLACE TEMPORARY VIEW temp_vw
AS
SELECT * FROM dept;

SELECT * FROM temp_vw;

SELECT * FROM ( 
		SELECT deptno, count(*) emp_count
        	FROM emp
                GROUP BY deptno ) emp,
         dept dept
WHERE dept.deptno = emp.deptno
ORDER BY dept.deptno;

--Cleanup

--DROP SYNONYM syn_dept;

DROP VIEW smpl_vw;

DROP VIEW comp_vw;

DROP VIEW temp_vw;

DROP FOREIGN TABLE weblogs;

DROP FOREIGN TABLE emp;

DROP FOREIGN TABLE dept;

DROP FOREIGN TABLE jobhist;

DROP TABLE dept_lcl;

DROP EXTENSION hdfs_fdw CASCADE;

\c postgres postgres

DROP DATABASE fdw_regression;
