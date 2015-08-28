CREATE DATABASE testdb;
USE testdb;
DROP TABLE department;
DROP TABLE employee;

CREATE TABLE department(department_id int, department_name string);
CREATE TABLE employee(emp_id int, emp_name string, emp_dept_id int);
LOAD DATA INPATH 'emp.txt' OVERWRITE INTO TABLE employee;
LOAD DATA INPATH 'dept.txt' OVERWRITE INTO TABLE department;
