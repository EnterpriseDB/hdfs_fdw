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

#include "libhive/odbc/hiveclient.h"


#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"
#include "access/htup.h"

#define HDFS_TINYINT     0
#define HDFS_SMALLINT    1
#define HDFS_INT         2
#define HDFS_BIGINT      3
#define HDFS_STRING      4
#define HDFS_VARCHAR     5
#define HDFS_CHAR        6
#define HDFS_TIMESTAMPS  7
#define HDFS_DACIMAL     8
#define HDFS_DATE        9
#define HDFS_DOUBLE      10
#define HDFS_BOLEAN      11
typedef enum CLIENT_TYPE
{
	HIVESERVER1,
	HIVESERVER2
} CLIENT_TYPE;

typedef struct hdfs_col
{
	char *col_name;
	int  col_type;
} hdfs_column;


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
	bool           use_remote_estimate;
	int            connect_timeout;
	int            receive_timeout;
} hdfs_opt;

typedef struct hdfsFdwExecutionState
{
	char            *query;
	MemoryContext batch_cxt;
	HiveResultSet   *result;
	HiveConnection *conn;
	Relation        rel;              /* relcache entry for the foreign table */
	List	        *retrieved_attrs; /* list of retrieved attribute numbers */
	List	        *col_list;        /* list of remote column's type */
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
	bool		use_remote_estimate;
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
	unsigned int cursor_number;   /* quasi-unique ID for my cursor */
	bool         cursor_exists;   /* have we created the cursor? */
	int          numParams;       /* number of parameters passed to query */
	FmgrInfo     *param_flinfo;   /* output conversion functions for them */
	List         *param_exprs;    /* executable expressions for param values */
	const char   **param_values;  /* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple  *tuples;         /* array of currently-retrieved tuples */
	int        num_tuples;     /* # of tuples in array */
	int        next_tuple;     /* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int   fetch_ct_2;     /* Min(# of fetches done, 2) */
	bool  eof_reached;    /* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;    /* context holding current batch of tuples */
	MemoryContext temp_cxt;     /* context for per-tuple temporary data */
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
HiveConnection *hdfs_get_connection(ForeignServer *server, UserMapping *user, hdfs_opt *opt);
void hdfs_rel_connection(HiveConnection *conn);

/* Functions prototypes for hdfs_deparse.c file */
extern void hdfs_deparse_select(hdfs_opt *opt, StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, Bitmapset *attrs_used, List **retrieved_attrs);
extern void hdfs_append_where_clause(hdfs_opt *opt, StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, List *exprs, bool is_first, List **params);
extern void classifyConditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds, List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern void deparseAnalyzeSql(hdfs_opt *opt, StringInfo buf, Relation rel, List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);

List *hdfs_desc_query(HiveConnection *conn, hdfs_opt *opt);

void hdfs_deparse_describe(StringInfo buf, hdfs_opt *opt);
void hdfs_deparse_explain(hdfs_opt *opt, StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo);
void hdfs_deparse_analyze(StringInfo buf, hdfs_opt *opt);

size_t hdfs_get_column_count(hdfs_opt *opt, HiveResultSet *rs);
size_t hdfs_get_field_data_len(hdfs_opt *opt, HiveResultSet *rs, int col);
HiveReturn hdfs_fetch(hdfs_opt *opt, HiveResultSet *rs);
char* hdfs_get_field_as_cstring(hdfs_opt *opt, HiveResultSet *rs, int idx, bool *is_null, int len);

Datum
hdfs_get_value(hdfs_opt *opt, Oid pgtyp, int pgtypmod, HiveResultSet *rs, int idx, bool *is_null, int len, int col_type);

HiveResultSet* hdfs_query_execute(HiveConnection *conn, hdfs_opt *opt, char *query);
void hdfs_close_result_set(hdfs_opt *opt, HiveResultSet *rs);

double hdfs_rowcount(HiveConnection *conn, hdfs_opt *opt, PlannerInfo *root, RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo);
double hdfs_describe(HiveConnection *conn, hdfs_opt *opt);
void hdfs_analyze(HiveConnection *conn, hdfs_opt *opt);

#endif   /* HADOOP_FDW_H */
