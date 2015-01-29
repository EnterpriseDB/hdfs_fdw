\c postgres
CREATE EXTENSION hdfs_fdw;
CREATE SERVER hdfs_svr FOREIGN DATA WRAPPER hdfs_fdw;
CREATE USER MAPPING FOR postgres SERVER hdfs_svr;

CREATE FOREIGN TABLE department(department_id int, department_name text) SERVER hdfs_svr OPTIONS(dbname 'testdb', table_name 'department');
CREATE FOREIGN TABLE employee(emp_id int, emp_name text, emp_dept_id int) SERVER hdfs_svr OPTIONS(dbname 'testdb', table_name 'employee');

SELECT * FROM department LIMIT 10;
SELECT * FROM employee LIMIT 10;

SELECT count(*) FROM department;
SELECT count(*) FROM employee;

EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;

EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;

SELECT * FROM employee LIMIT 10;
SELECT * FROM employee WHERE emp_id IN (1);
SELECT * FROM employee WHERE emp_id IN (1,3,4,5);
SELECT * FROM employee WHERE emp_id IN (10000,1000);

SELECT * FROM employee WHERE emp_id NOT IN (1) LIMIT 5;
SELECT * FROM employee WHERE emp_id NOT IN (1,3,4,5);
SELECT * FROM employee WHERE emp_id NOT IN (1,4);

SELECT * FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10));
SELECT * FROM employee WHERE emp_name NOT IN ('emp-1', 'emp-2');
SELECT * FROM employee WHERE emp_name NOT IN ('emp-10');

DROP FOREIGN TABLE department;
DROP FOREIGN TABLE employee;
DROP USER MAPPING FOR postgres SERVER hdfs_svr;
DROP SERVER hdfs_svr;
DROP EXTENSION hdfs_fdw CASCADE;
