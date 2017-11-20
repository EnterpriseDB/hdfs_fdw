CREATE DATABASE testdb;
USE testdb;
DROP TABLE department;
DROP TABLE employee;

CREATE TABLE department(department_id int, department_name string);
CREATE TABLE employee(emp_id int, emp_name string, emp_dept_id int);
LOAD DATA INPATH 'employee_table.txt' OVERWRITE INTO TABLE employee;
LOAD DATA INPATH 'department_table.txt' OVERWRITE INTO TABLE department;

--QA DB and Table Setup.

CREATE DATABASE fdw_db;
USE fdw_db;

DROP TABLE dept;
DROP TABLE emp;
DROP TABLE jobhist;
DROP TABLE weblogs;

CREATE TABLE dept (
    deptno          INT,
    dname           VARCHAR(14),
    loc             VARCHAR(13)
)
row format delimited fields terminated by ',';

CREATE TABLE emp (
    empno           INT,
    ename           VARCHAR(10),
    job                 VARCHAR(9),
    mgr               INT,
    hiredate        DATE,
    sal                INT,
    comm           DECIMAL(7,2),
    deptno          INT
)
row format delimited fields terminated by ',';

CREATE TABLE jobhist (
    empno           INT,
    startdate       DATE,
    enddate         DATE,
    job                VARCHAR(29),
    sal                DECIMAL(7,2),
    comm            DECIMAL(7,2),
    deptno          INT,
    chgdesc         VARCHAR(80)
)
row format delimited fields terminated by ',';

CREATE TABLE weblogs (
    client_ip           STRING,
    full_request_date   STRING,
    day                 STRING,
    month               STRING,
    month_num           INT,
    year                STRING,
    hour                STRING,
    minute              STRING,
    second              STRING,
    timezone            STRING,
    http_verb           STRING,
    uri                 STRING,
    http_status_code    STRING,
    bytes_returned      STRING,
    referrer            STRING,
    user_agent          STRING)
row format delimited
fields terminated by '\t';

CREATE EXTERNAL TABLE emp_ext (
    empno           INT,
    ename           VARCHAR(10),
    job                 VARCHAR(9),
    mgr               INT,
    hiredate        DATE,
    sal                INT,
    comm           DECIMAL(7,2),
    deptno          INT
)
row format delimited fields terminated by ','
location '/home/hadoop/empext/';

-- Tables for Datatype Mappings

CREATE TABLE dept_dt_mp (
    deptno          TINYINT,
    dname          STRING,
    loc                 CHAR(13)
)
row format delimited fields terminated by ',';

CREATE TABLE dept_dt_mp1 (
    deptno          BIGINT,
    dname          STRING,
    loc                 CHAR(13)
)
row format delimited fields terminated by ',';

CREATE TABLE emp_dt_mp1 (
    empno           SMALLINT,
    ename           VARCHAR(10),
    job                 VARCHAR(9),
    mgr               INT,
    hiredate        TIMESTAMP,
    sal                DOUBLE,
    comm           DECIMAL(7,2),
    deptno          INT
)
row format delimited fields terminated by ',';

CREATE TABLE bool_test (
    bol1              	BOOLEAN,
    bol2               INT
)
row format delimited fields terminated by ',';

CREATE TABLE str_date (
    col1              	string
)
row format delimited fields terminated by ',';

CREATE TABLE chr_date (
    col1              	char(30)
)
row format delimited fields terminated by ',';

CREATE TABLE var_date (
    col1         VARCHAR(30)
)
row format delimited fields terminated by ',';

CREATE TABLE var_int (
    col1         VARCHAR(30)
)
row format delimited fields terminated by ',';

CREATE TABLE str_int (
    col1         string
)
row format delimited fields terminated by ',';

CREATE TABLE array_data 
(
    id         INT,
    name   STRING,
    sal        decimal,
    sub      array<string>    
)
row format delimited fields terminated by ','
collection items terminated by '$';

CREATE TABLE emp_dt_mp2 (
    empno           SMALLINT,
    ename           VARCHAR(10),
    job                 VARCHAR(9),
    mgr               INT,
    hiredate        TIMESTAMP,
    sal                float,
    comm           DECIMAL(7,2),
    deptno          INT
)
row format delimited fields terminated by ',';

CREATE TABLE binary_data (
    col1         binary
)
row format delimited fields terminated by ',';

--CREATE TABLE emp (
--    empno           INT,
--    ename           VARCHAR(10),
--    job                 VARCHAR(9),
--    mgr               INT,
--    hiredate        DATE,
--    sal                INT,
--    comm           DECIMAL(7,2),
--    deptno          INT
--)
--row format delimited fields terminated by ',';

CREATE TABLE double_data (
    col1         DOUBLE
)
row format delimited fields terminated by ',';

CREATE TABLE emp2 (
    empno           INT,
    ename           VARCHAR(10),
    job                 VARCHAR(9),
    mgr               INT,
    hiredate        DATE,
    sal                INT,
    comm           INT,
    deptno          INT
)
row format delimited fields terminated by ',';

--Load data in the Tables.

LOAD DATA INPATH 'emp_table.txt' OVERWRITE INTO TABLE emp;
LOAD DATA INPATH 'emp_table_2.txt' OVERWRITE INTO TABLE emp2;
LOAD DATA INPATH 'dept_table.txt' OVERWRITE INTO TABLE dept;
LOAD DATA INPATH 'jobhist_table.txt' OVERWRITE INTO TABLE jobhist;
LOAD DATA INPATH 'weblogs_parse_table.txt' OVERWRITE INTO TABLE weblogs;
LOAD DATA INPATH 'empext/emp_ext_table.txt' OVERWRITE INTO TABLE emp_ext;
--Tables for datatype mappings.
LOAD DATA INPATH 'dept_table.txt' OVERWRITE INTO TABLE dept_dt_mp;
LOAD DATA INPATH 'dept_table.txt' OVERWRITE INTO TABLE dept_dt_mp1;
LOAD DATA INPATH 'emp_1_table.txt' OVERWRITE INTO TABLE emp_dt_mp1;
LOAD DATA INPATH 'bool_dt_table.txt' OVERWRITE INTO TABLE bool_test;
LOAD DATA INPATH 'str_dt_table.txt' OVERWRITE INTO TABLE str_date;
LOAD DATA INPATH 'str_dt_table.txt' OVERWRITE INTO TABLE chr_date;
LOAD DATA INPATH 'str_dt_table.txt' OVERWRITE INTO TABLE var_date;
LOAD DATA INPATH 'num_dt_table.txt' OVERWRITE INTO TABLE var_int;
LOAD DATA INPATH 'num_dt_table.txt' OVERWRITE INTO TABLE str_int;
LOAD DATA INPATH 'arrdat_dt_table.txt' OVERWRITE INTO TABLE array_data;
LOAD DATA INPATH 'emp_1_table.txt' OVERWRITE INTO TABLE emp_dt_mp2;
LOAD DATA INPATH 'emp_1_table.txt' OVERWRITE INTO TABLE binary_data;
LOAD DATA INPATH 'dept_table2.txt' OVERWRITE INTO TABLE dept_dt_mp1;
LOAD DATA INPATH 'dept_table1.txt' OVERWRITE INTO TABLE dept_dt_mp1;
LOAD DATA INPATH 'str_dt_table1.txt' OVERWRITE INTO TABLE chr_date;
LOAD DATA INPATH 'str_dt_table2.txt' OVERWRITE INTO TABLE var_date;
LOAD DATA INPATH 'num_dt_table1.txt' OVERWRITE INTO TABLE str_int;
LOAD DATA INPATH 'emp_1_table1.txt' OVERWRITE INTO TABLE emp_dt_mp2;
LOAD DATA INPATH 'emp_1_table2.txt' OVERWRITE INTO TABLE binary_data;
