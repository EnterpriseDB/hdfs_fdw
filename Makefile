##############################################################
# Makefile
#
# Portions Copyright © 2004-2014, EnterpriseDB Corporation.
#

MODULE_big = hdfs_fdw
CCC=g++
HIVECLIENT_HOME=libhive
THRIFT_HOME:="$(shell echo $(THRIFT_HOME))"

PG_CPPFLAGS = -Wno-unused-variable -I$(HIVECLIENT_HOME)  -I$(HIVECLIENT_HOME)/odbc  -I$(HIVECLIENT_HOME)/odbc2 -DHAVE_NETINET_IN_H -DHAVE_INTTYPES_H 
SHLIB_LINK = -L$(THRIFT_HOME)/lib -L$(HIVECLIENT_HOME) -lhive  -lfb303 -lstdc++ -lthrift

OBJS = hdfs_client.o hdfs_query.o hdfs_option.o hdfs_deparse.o hdfs_connection.o hdfs_fdw.o

REGRESS =  hdfs_fdw_hive2
EXTENSION = hdfs_fdw
DATA = hdfs_fdw--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif

ifeq (,$(findstring $(MAJORVERSION), 9.3 9.4 9.5 9.6))
    $(error PostgreSQL 9.3, 9.4, 9.5 or 9.6 is required to compile this extension)
endif
