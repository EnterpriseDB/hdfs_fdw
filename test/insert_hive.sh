#!/bin/sh
hadoop fs -rm employee_table.txt
hadoop fs -rm department_table.txt
hadoop fs -put employee_table.txt 
hadoop fs -put department_table.txt
# QA Test Data
hadoop fs -rm emp_table.txt
hadoop fs -rm dept_table.txt
hadoop fs -rm jobhist_table.txt
hadoop fs -rm weblogs_parse_table.txt
hadoop fs -rm empext/emp_ext_table.txt
hadoop fs -rm emp_1_table.txt
hadoop fs -rm bool_dt_table.txt
hadoop fs -rm str_dt_table.txt
hadoop fs -rm num_dt_table.txt
hadoop fs -rm arrdat_dt_table.txt
hadoop fs -put emp_table.txt
hadoop fs -put dept_table.txt
hadoop fs -put jobhist_table.txt
hadoop fs -put weblogs_parse_table.txt
hadoop fs -put empext/emp_ext_table.txt
hadoop fs -put emp_1_table.txt
hadoop fs -put bool_dt_table.txt
hadoop fs -put str_dt_table.txt
hadoop fs -put num_dt_table.txt
hadoop fs -put arrdat_dt_table.txt
#hive 127.0.0.1 -f hive.sql
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f hive.sql
