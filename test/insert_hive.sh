#!/bin/sh
hadoop fs -rm emp.txt
hadoop fs -rm dept.txt
hadoop fs -put emp.txt 
hadoop fs -put dept.txt
hive -h 127.0.0.1 -f hive.sql
