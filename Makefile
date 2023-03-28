##############################################################
# Makefile
#
# Portions Copyright (c) 2004-2023, EnterpriseDB Corporation.
#
##############################################################

MODULE_big = hdfs_fdw
CCC=g++
HIVECLIENT_HOME=libhive
THRIFT_HOME:="$(shell echo $(THRIFT_HOME))"

PG_CPPFLAGS = -Wno-unused-variable -I$(JDK_INCLUDE) -I$(JDK_INCLUDE)/linux/  -I$(HIVECLIENT_HOME) -DHAVE_NETINET_IN_H -DHAVE_INTTYPES_H
SHLIB_LINK := -L$(HIVECLIENT_HOME) -lhive -lstdc++ -L$(JDK_INCLUDE) $(LDFLAGS)


OBJS = hdfs_client.o hdfs_query.o hdfs_option.o hdfs_deparse.o hdfs_connection.o hdfs_fdw.o

REGRESS = datatype external mapping retrieval date_comparison ldap_authentication remote_estimates log_remote_sql where_push_down misc where_push_down_normal_queries auth_client_type_parameters join_pushdown aggregate_pushdown order_by_pushdown upperrel_final_pushdown
EXTENSION = hdfs_fdw
DATA = hdfs_fdw--2.0.5.sql hdfs_fdw--2.0.4--2.0.5.sql hdfs_fdw--2.0.4.sql hdfs_fdw--2.0.3--2.0.4.sql hdfs_fdw--2.0.2.sql hdfs_fdw--2.0.3.sql hdfs_fdw--2.0.1--2.0.2.sql hdfs_fdw--2.0.2--2.0.3.sql hdfs_fdw--2.0.1.sql hdfs_fdw--2.0--2.0.1.sql hdfs_fdw--1.0--2.0.sql hdfs_fdw--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
	MAJORVERSION := $(basename $(VERSION))
endif

ifeq (,$(findstring $(MAJORVERSION), 11 12 13 14 15))
$(error PostgreSQL 11, 12, 13, 14, or 15 is required to compile this extension)
endif
