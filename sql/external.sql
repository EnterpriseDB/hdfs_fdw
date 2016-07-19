/*---------------------------------------------------------------------------------------------------------------------
 *
 * external.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *              To verify that Data is retrived correctly from Hadoop Foreign Tables based on Hadoop External Table.
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		external.sql
 *
 *---------------------------------------------------------------------------------------------------------------------
 */

-- Connection Settings.

\set HIVE_SERVER                '\'hive.server\''
\set HIVE_CLIENT_TYPE           '\'hiveserver2\''
\set HIVE_PORT                  '\'10000\''

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

-- Create Hadoop USER MAPPING.

CREATE USER MAPPING FOR postgres SERVER hdfs_server;

-- Create Foreign Tables.

CREATE FOREIGN TABLE emp_ext (
empno INTEGER,
ename VARCHAR2(10),
job VARCHAR2(9),
mgr INTEGER,
hiredate DATE,
sal INTEGER,
comm INTEGER,
deptno INTEGER
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'emp_ext');

CREATE FOREIGN TABLE dept (
    deptno          INTEGER,
    dname           VARCHAR2(14),
    loc             VARCHAR2(13)
)
SERVER hdfs_server OPTIONS (dbname 'fdw_db', table_name 'dept');

-- Create the Stored Procedure to display the Remote Query.

CREATE OR REPLACE PROCEDURE checkqryplan(query TEXT) AS
  plan             TEXT := '';
  partname         TEXT := '';
  qry		   TEXT;
BEGIN
  FOR step IN EXECUTE 'EXPLAIN VERBOSE ' || query LOOP
	  
 	IF (step LIKE '%Remote SQL%') THEN
                partname := rtrim(substring(step from position('Remote SQL:' in step)),'{")}');
  		DBMS_OUTPUT.SERVEROUTPUT(TRUE);
		DBMS_OUTPUT.PUT_LINE(partname);
	  END IF;
  END LOOP;
END;

-- Data retrival using SELECT Statement.

SELECT * FROM emp_ext;

SELECT empno, ename, job, mgr, hiredate, sal, comm, deptno FROM emp_ext;

SELECT * FROM emp_ext ORDER BY deptno;

SELECT DISTINCT mgr, deptno FROM emp_ext;

SELECT deptno, sal, comm FROM emp_ext ORDER BY deptno;

SELECT ename as "Employee Name" FROM emp_ext;

SELECT COUNT(*) FROM emp_ext;

SELECT * FROM emp_ext ORDER BY empno LIMIT 2;

SELECT * FROM emp_ext ORDER BY empno LIMIT 5 OFFSET 1;

-- Data retrival using Group By.

SELECT deptno "Department", COUNT(emp_ext) "Total Employees" FROM emp_ext GROUP BY deptno ORDER BY deptno;

SELECT deptno, ROUND(AVG(sal)) "Average Salary", SUM(sal) "Total Salary", MIN(sal) "Minimum Salary", MAX(sal) "Maximum Salary" FROM emp_ext
GROUP BY deptno
HAVING deptno = 10 OR deptno = 20 OR deptno = 30
ORDER BY deptno;

-- Data retrival using Sub Queries.

SELECT * FROM emp_ext
WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno;

SELECT * FROM emp_ext
WHERE deptno NOT IN (SELECT deptno FROM dept)
ORDER BY empno;

-- Data retrival using UNION.

SELECT deptno,dname FROM dept 
UNION
SELECT empno,ename FROM emp_ext
ORDER BY deptno;

-- Data retrival using INTERSECT.

SELECT ename FROM emp_ext WHERE empno >= 7788 
INTERSECT
SELECT ename FROM emp_ext WHERE empno >= 7566
ORDER BY ename;

-- Data retrival using CROSS JOIN.

SELECT dept.dname, emp_ext.ename FROM dept CROSS JOIN emp_ext
ORDER BY dept.deptno;

-- Data retrival using INNER JOIN.

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d, emp_ext e
WHERE d.deptno=e.deptno
ORDER BY d.deptno;

-- Data retrival using EXCEPT.

SELECT ename FROM emp_ext 
EXCEPT
SELECT ename FROM emp_ext WHERE empno > 7839
ORDER BY ename;

-- Data retrival using OUTER JOINS.

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d LEFT OUTER JOIN emp_ext e
ON d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d RIGHT OUTER JOIN emp_ext e ON d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept d FULL OUTER JOIN emp_ext e
ON d.deptno=e.deptno
ORDER BY d.deptno;

-- Data retrival using CTE (With Clause).

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
ORDER BY e.deptno, e.ename;

-- Data retrival using Window Clause.

SELECT deptno, empno, sal, AVG(sal) OVER (PARTITION BY deptno) 
FROM emp_ext
ORDER BY deptno, empno;

SELECT deptno, empno, sal, COUNT(sal) OVER (PARTITION BY deptno) 
FROM emp_ext
WHERE deptno IN (10,30,40,50,60,70)
ORDER BY deptno, empno;

-- Data retrival using Query Optimizer.

EXPLAIN (COSTS OFF) SELECT * FROM emp_ext ORDER BY deptno;

EXEC checkqryplan($$SELECT * FROM emp_ext ORDER BY deptno$$);

EXPLAIN (COSTS OFF) SELECT empno,ename FROM emp_ext ORDER BY deptno;

EXEC checkqryplan($$SELECT empno,ename FROM emp_ext ORDER BY deptno$$);

EXPLAIN (COSTS OFF) SELECT DISTINCT deptno FROM emp_ext;

EXEC checkqryplan($$SELECT DISTINCT deptno FROM emp_ext$$);

EXPLAIN (COSTS OFF) SELECT * FROM emp_ext LIMIT 2;

EXEC checkqryplan($$SELECT * FROM emp_ext LIMIT 2$$);

EXPLAIN (COSTS OFF) SELECT deptno "Department", SUM(sal) "Total Salary…." FROM emp_ext
GROUP BY deptno
HAVING SUM(sal) > 8750
ORDER BY deptno;

EXEC checkqryplan($$SELECT deptno "Department", SUM(sal) "Total Salary…." FROM emp_ext
GROUP BY deptno
HAVING SUM(sal) > 8750
ORDER BY deptno$$);

EXPLAIN (COSTS OFF) SELECT * FROM emp_ext
WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno;

EXEC checkqryplan($$SELECT * FROM emp_ext
WHERE deptno <> ALL (SELECT deptno FROM dept WHERE deptno IN (10,30,40))
ORDER BY empno$$);

EXPLAIN (COSTS OFF) 
SELECT deptno,dname FROM dept 
UNION
SELECT empno,ename FROM emp_ext
ORDER BY deptno;

EXEC checkqryplan($$SELECT deptno,dname FROM dept 
UNION
SELECT empno,ename FROM emp_ext
ORDER BY deptno$$);

-- Joins with Foreign and Local Tables.

CREATE TABLE dept_lcl (
deptno NUMBER(2) NOT NULL CONSTRAINT dept_pk PRIMARY KEY,
dname VARCHAR2(14) CONSTRAINT dept_dname_uq UNIQUE,
loc VARCHAR2(13)
);

INSERT INTO dept_lcl VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept_lcl VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept_lcl VALUES (30,'SALES','CHICAGO');
INSERT INTO dept_lcl VALUES (40,'OPERATIONS','BOSTON');

SELECT dept_lcl.dname, emp_ext.ename FROM dept_lcl CROSS JOIN emp_ext
ORDER BY dept_lcl.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d, emp_ext e
WHERE d.deptno=e.deptno
ORDER BY d.deptno;

SELECT d.deptno,d.dname,e.empno,e.ename,e.sal,e.deptno FROM dept_lcl d LEFT OUTER JOIN emp_ext e
ON d.deptno=e.deptno
ORDER BY d.deptno;

EXPLAIN (COSTS OFF) SELECT dept_lcl.dname, emp_ext.ename FROM dept_lcl CROSS JOIN emp_ext
ORDER BY dept_lcl.deptno;

EXEC checkqryplan($$SELECT dept_lcl.dname, emp_ext.ename FROM dept_lcl CROSS JOIN emp_ext
ORDER BY dept_lcl.deptno$$);


--Cleanup


DROP FOREIGN TABLE emp_ext;

DROP FOREIGN TABLE dept;

DROP TABLE dept_lcl;

DROP EXTENSION hdfs_fdw CASCADE;

DROP PROCEDURE checkqryplan;

\c postgres postgres

DROP DATABASE fdw_regression;
