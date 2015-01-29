#!/bin/bash
rm -rf thrift odbc ql metastore
svn checkout https://svn.apache.org/repos/asf/hive/trunk/service/src/gen/thrift/gen-cpp/ thrift
svn checkout https://svn.apache.org/repos/asf/hive/trunk/odbc/src/cpp odbc
svn checkout https://svn.apache.org/repos/asf/hive/trunk/ql/src/gen/thrift/gen-cpp/ ql
svn checkout https://svn.apache.org/repos/asf/hive/trunk/metastore/src/gen/thrift/gen-cpp/ metastore


