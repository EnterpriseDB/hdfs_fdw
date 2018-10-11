/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.h
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw.h
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef HADOOP_FDW_H
#define HADOOP_FDW_H

#include "libhive/jdbc/hiveclient.h"


#include "access/tupdesc.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"
#include "access/htup.h"

/* Default connection parameters */
/* Default Hive database name */
static const char* DEFAULT_DATABASE = "default";
/* Default Hive Server host */
static const char* DEFAULT_HOST     = "localhost";
/* Default Hive Server port */
static const char* DEFAULT_PORT     = "10000";



/*
 * Options structure to store the HDFS server information */
typedef struct hdfs_opt
{
	int            port;              /* HDFS port number */
	char           *host;             /* HDFS server IP address */
	char           *username;         /* HDFS user name */
	char           *password;         /* HDFS password */
	char           *dbname;           /* HDFS database name */
	char           *table_name;       /* HDFS table name */
	CLIENT_TYPE    client_type;
	AUTH_TYPE      auth_type;
	bool           use_remote_estimate;
	int            connect_timeout;
	int            receive_timeout;
	int            fetch_size;
	bool           log_remote_sql;
} hdfs_opt;

typedef struct hdfsFdwExecutionState
{
	char            *query;
	MemoryContext batch_cxt;
	bool			query_executed;
	int				con_index;
	Relation        rel;              /* relcache entry for the foreign table */
	List	        *retrieved_attrs; /* list of retrieved attribute numbers */

	/* for remote query execution */
	int          numParams;         /* number of parameters passed to query */
	List         *param_exprs;      /* executable expressions for param values */
	Oid          *param_types;      /* type of query parameters */

	int			rescan_count;		/* Number of times a foreign scan is restarted */
} hdfsFdwExecutionState;


/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by postgresGetForeignRelSize.
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
	double      rows;
	int         width;
	Cost        startup_cost;
	Cost        total_cost;

	/* Options extracted from catalogs. */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;

	/* Cached catalog information. */
	ForeignTable  *table;
	ForeignServer *server;
	UserMapping   *user;			/* only set in use_remote_estimate mode */
} HDFSFdwRelationInfo;


/*
 * Execution state of a foreign scan using postgres_fdw.
 */
typedef struct HDFSFdwScanState
{
	Relation   rel;               /* relcache entry for the foreign table */

	/* extracted fdw_private data */
	char       *query;            /* text of SELECT command */
	List       *retrieved_attrs;  /* list of retrieved attribute numbers */

	/* for remote query execution */
	int          numParams;         /* number of parameters passed to query */
	List         *param_exprs;      /* executable expressions for param values */
	Oid          *param_types;      /* type of query parameters */

	/* working memory contexts */
	MemoryContext batch_cxt;    /* context holding current batch of tuples */
} HDFSFdwScanState;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr *current;       /* current expr, or NULL if not yet found */
	List *already_used; /* expressions already dealt with */
} ec_member_foreign_arg;

/* Functions prototypes for hdfs_option.c file */
hdfs_opt* hdfs_get_options(Oid foreigntableid);

/* Functions prototypes for hdfs_connection.c file */
int hdfs_get_connection(ForeignServer *server, UserMapping *user, hdfs_opt *opt);
void hdfs_rel_connection(int con_index);

/* Functions prototypes for hdfs_deparse.c file */
extern void hdfs_deparse_select(hdfs_opt *opt, StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, Bitmapset *attrs_used, List **retrieved_attrs);
extern void hdfs_append_where_clause(hdfs_opt *opt, StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, List *exprs, bool is_first, List **params);
extern void classifyConditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds, List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern void deparseAnalyzeSql(hdfs_opt *opt, StringInfo buf, Relation rel, List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);

void hdfs_deparse_describe(StringInfo buf, hdfs_opt *opt);
void hdfs_deparse_explain(hdfs_opt *opt, StringInfo buf,
						PlannerInfo *root, RelOptInfo *baserel,
						HDFSFdwRelationInfo *fpinfo);
void hdfs_deparse_analyze(StringInfo buf, hdfs_opt *opt);
double hdfs_find_row_count(char *src);
int hdfs_get_column_count(int con_index, hdfs_opt *opt);
int hdfs_fetch(int con_index, hdfs_opt *opt);
char* hdfs_get_field_as_cstring(int con_index, hdfs_opt *opt, int idx, bool *is_null);

Datum hdfs_get_value(int con_index, hdfs_opt *opt, Oid pgtyp, int pgtypmod,
					int idx, bool *is_null);

bool hdfs_query_execute(int con_index, hdfs_opt *opt, char *query);
bool hdfs_query_prepare(int con_index, hdfs_opt *opt, char *query);
bool hdfs_execute_prepared(int con_index);
bool hdfs_query_execute_utility(int con_index, hdfs_opt *opt, char *query);
void hdfs_close_result_set(int con_index, hdfs_opt *opt);
void hdfs_rewind_result_set(int con_index, hdfs_opt *opt);

double hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
					RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo);
double hdfs_describe(int con_index, hdfs_opt *opt);
void hdfs_analyze(int con_index, hdfs_opt *opt);
bool hdfs_bind_var(int con_index, int param_index, Oid type,
								Datum value, bool *isnull);

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

extern void _PG_init(void);
extern void _PG_fini(void);

#endif   /* HADOOP_FDW_H */
