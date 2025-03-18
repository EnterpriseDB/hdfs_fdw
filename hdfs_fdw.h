/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.h
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
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
#include "nodes/pathnodes.h"
#include "utils/rel.h"

/*
 * Default database name if, the dbname option is not provided either in
 * connection or table options.
 */
#define DEFAULT_DATABASE "default"

/* Macro for list API backporting. */
#if PG_VERSION_NUM < 130000
#define hdfs_list_concat(l1, l2) list_concat(l1, list_copy(l2))
#else
#define hdfs_list_concat(l1, l2) list_concat((l1), (l2))
#endif

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
	bool		enable_join_pushdown;
	bool		enable_aggregate_pushdown;
	bool		enable_order_by_pushdown;
} hdfs_opt;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by hdfsGetForeignRelSize.
 */
typedef struct HDFSFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

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

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo	relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	List	   *joinclauses;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */

	/* Grouping information */
	List	   *grouped_tlist;

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;

	hdfs_opt   *options;		/* Options applicable for this relation */

	/* Upper relation information */
	UpperRelationKind stage;

	/* Inherit required flags from hdfs_opt */
	bool		enable_aggregate_pushdown;
	bool		enable_order_by_pushdown;
	CLIENT_TYPE client_type;
} HDFSFdwRelationInfo;

/* hdfs_option.c headers */
extern hdfs_opt *hdfs_get_options(Oid foreigntableid);

/* hdfs_connection.c headers */
extern int	hdfs_get_connection(ForeignServer *server, hdfs_opt *opt);
extern void hdfs_rel_connection(int con_index);

/* hdfs_deparse.c headers */
extern void hdfs_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root,
											 RelOptInfo *rel, List *tlist,
											 List *remote_conds,
											 bool is_subquery,
											 List *pathkeys,
											 bool has_final_sort,
											 bool has_limit,
											 List **retrieved_attrs,
											 List **params_list);
extern void hdfs_classify_conditions(PlannerInfo *root, RelOptInfo *baserel,
									 List *input_conds, List **remote_conds,
									 List **local_conds);
extern bool hdfs_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
								 Expr *expr, bool is_remote_cond);
extern void hdfs_deparse_describe(StringInfo buf, Relation rel);
extern void hdfs_deparse_explain(hdfs_opt *opt, StringInfo buf);
extern void hdfs_deparse_analyze(StringInfo buf, Relation rel);
extern bool hdfs_is_foreign_param(PlannerInfo *root, RelOptInfo *baserel,
								  Expr *expr);
extern bool hdfs_is_foreign_pathkey(PlannerInfo *root,
									RelOptInfo *baserel,
									PathKey *pathkey);
extern char *hdfs_get_sortby_direction_string(EquivalenceMember *em,
											  PathKey *pathkey);
extern EquivalenceMember *hdfs_find_em_for_rel(PlannerInfo *root,
											   EquivalenceClass *ec,
											   RelOptInfo *rel);
extern EquivalenceMember *hdfs_find_em_for_rel_target(PlannerInfo *root,
													  EquivalenceClass *ec,
													  RelOptInfo *rel);
extern bool hdfs_is_builtin(Oid objectId);

/* hdfs_query.c headers */
extern double hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
							RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo);
extern double hdfs_describe(int con_index, hdfs_opt *opt, Relation rel);
extern void hdfs_analyze(int con_index, hdfs_opt *opt, Relation rel);
extern const char *hdfs_get_jointype_name(JoinType jointype);

/* hdfs_client.c headers */
extern int	hdfs_get_column_count(int con_index);
extern int	hdfs_fetch(int con_index);
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

/* hdfs_fdw.c headers */
extern List *hdfs_adjust_whole_row_ref(PlannerInfo *root,
									   List *scan_var_list,
									   List **whole_row_lists,
									   Bitmapset *relids);
#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#endif							/* HADOOP_FDW_H */
