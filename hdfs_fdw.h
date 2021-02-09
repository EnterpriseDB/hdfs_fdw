/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.h
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HADOOP_FDW_H
#define HADOOP_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "libhive/jdbc/hiveclient.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif
#include "utils/rel.h"

/* Options structure to store the HDFS server information */
typedef struct hdfs_opt
{
	int			port;			/* Hive/Spark Server port number */
	char	   *host;			/* Hive/Spark server IP address */
	char	   *username;		/* Hive/Spark user name */
	char	   *password;		/* Hive/Spark password */
	char	   *dbname;			/* Hive/Spark database name */
	char	   *table_name;		/* Hive/Spark table name */
	CLIENT_TYPE client_type;
	AUTH_TYPE	auth_type;
	bool		use_remote_estimate;
	int			connect_timeout;
	int			receive_timeout;
	int			fetch_size;
	bool		log_remote_sql;
} hdfs_opt;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by hdfsGetForeignRelSize.
 */
typedef struct HDFSFdwRelationInfo
{
	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Estimated size and cost for a scan with baserestrictinfo quals. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Options extracted from catalogs. */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;			/* only set in use_remote_estimate mode */
} HDFSFdwRelationInfo;

/* hdfs_option.c headers */
extern hdfs_opt *hdfs_get_options(Oid foreigntableid);

/* hdfs_connection.c headers */
extern int hdfs_get_connection(ForeignServer *server, hdfs_opt *opt);
extern void hdfs_rel_connection(int con_index);

/* hdfs_deparse.c headers */
extern void hdfs_deparse_select(hdfs_opt *opt, StringInfo buf,
								PlannerInfo *root, RelOptInfo *baserel,
								Bitmapset *attrs_used,
								List **retrieved_attrs);
extern void hdfs_append_where_clause(hdfs_opt *opt, StringInfo buf,
									 PlannerInfo *root, RelOptInfo *baserel,
									 List *exprs, bool is_first,
									 List **params);
extern void hdfs_classify_conditions(PlannerInfo *root, RelOptInfo *baserel,
									 List *input_conds, List **remote_conds,
									 List **local_conds);
extern bool hdfs_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
								 Expr *expr);
extern void hdfs_deparse_describe(StringInfo buf, hdfs_opt *opt);
extern void hdfs_deparse_explain(hdfs_opt *opt, StringInfo buf);
extern void hdfs_deparse_analyze(StringInfo buf, hdfs_opt *opt);

/* hdfs_query.c headers */
extern double hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
							RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo);
extern double hdfs_describe(int con_index, hdfs_opt *opt);
extern void hdfs_analyze(int con_index, hdfs_opt *opt);

/* hdfs_client.c headers */
extern int hdfs_get_column_count(int con_index);
extern int hdfs_fetch(int con_index);
extern char *hdfs_get_field_as_cstring(int con_index, int idx, bool *is_null);
extern Datum hdfs_get_value(int con_index, hdfs_opt *opt, Oid pgtyp,
							int pgtypmod, int idx, bool *is_null);
extern bool hdfs_query_execute(int con_index, hdfs_opt *opt, char *query);
extern void hdfs_query_prepare(int con_index, hdfs_opt *opt, char *query);
extern bool hdfs_execute_prepared(int con_index);
extern bool hdfs_query_execute_utility(int con_index, hdfs_opt *opt,
									   char *query);
extern void hdfs_close_result_set(int con_index);
extern bool hdfs_bind_var(int con_index, int param_index, Oid type,
						  Datum value, bool *isnull);

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#endif							/* HADOOP_FDW_H */
