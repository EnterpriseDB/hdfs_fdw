/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "hdfs_fdw.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST    100000.0

/* Default CPU cost to process 1 row  */
#define DEFAULT_FDW_TUPLE_COST      1000.0

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 2.3.3 so number will be 20303
 */
#define CODE_VERSION   20303

/*
 * The number of rows in a foreign relation are estimated to be so less that
 * an in-memory sort on those many rows wouldn't cost noticeably higher than
 * the underlying scan. Hence for now, cost sorts same as underlying scans.
 */
#define DEFAULT_HDFS_SORT_MULTIPLIER 1

/* GUC variables. */
static bool enable_join_pushdown = true;
static bool enable_aggregate_pushdown = true;
static bool enable_order_by_pushdown = false;
static bool enable_limit_pushdown = true;

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum hdfsFdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, hdfsFdwScanPrivateSelectSql));
 */
enum hdfsFdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	hdfsFdwScanPrivateSelectSql,

	/* Integer list of attribute numbers retrieved by the SELECT */
	hdfsFdwScanPrivateRetrievedAttrs,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join.
	 */
	hdfsFdwScanPrivateRelations,

	/*
	 * List of Var node lists for constructing the whole-row references of
	 * base relations involved in pushed down join.
	 */
	hdfsFdwPrivateWholeRowLists,

	/*
	 * Targetlist representing the result fetched from the foreign server if
	 * whole-row references are involved.
	 */
	hdfsFdwPrivateScanTList
};

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/*
 * Structure to hold information for constructing a whole-row reference value
 * for a single base relation involved in a pushed down join.
 */
typedef struct
{
	/*
	 * Tuple descriptor for whole-row reference. We can not use the base
	 * relation's tuple descriptor as it is, since it might have information
	 * about dropped attributes.
	 */
	TupleDesc	tupdesc;

	/*
	 * Positions of the required attributes in the tuple fetched from the
	 * foreign server.
	 */
	int		   *attr_pos;

	/* Position of attribute indicating NULL-ness of whole-row reference */
	int			wr_null_ind_pos;

	/* Values and null array for holding column values. */
	Datum	   *values;
	bool	   *nulls;
} hdfsWRState;

typedef struct hdfsFdwExecutionState
{
	char	   *query;
	MemoryContext batch_cxt;
	bool		query_executed;
	int			con_index;
	Relation	rel;			/* relcache entry for the foreign table */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	/* For remote query execution. */
	int			numParams;		/* number of parameters passed to query */
	List	   *param_exprs;	/* executable expressions for param values */
	Oid		   *param_types;	/* type of query parameters */

	int			rescan_count;	/* number of times a foreign scan is restarted */
	AttInMetadata *attinmeta;

	/*
	 * Members used for constructing the ForeignScan result row when whole-row
	 * references are involved in a pushed down join.
	 */
	hdfsWRState **hdfswrstates; /* whole-row construction information for each
								 * base relation involved in the pushed down
								 * join. */
	int		   *wr_attrs_pos;	/* Array mapping the attributes in the
								 * ForeignScan result to those in the rows
								 * fetched from the foreign server. The array
								 * is indexed by the attribute numbers in the
								 * ForeignScan. */
	TupleDesc	wr_tupdesc;		/* Tuple descriptor describing the result of
								 * ForeignScan node. Should be same as that in
								 * ForeignScanState::ss::ss_ScanTupleSlot */
	/* Array for holding column values. */
	Datum	   *wr_values;
	bool	   *wr_nulls;
} hdfsFdwExecutionState;

extern void _PG_init(void);
extern void _PG_fini(void);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(hdfs_fdw_handler);
PG_FUNCTION_INFO_V1(hdfs_fdw_version);

/*
 * FDW callback routines
 */
static void hdfsGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								  Oid foreigntableid);
static void hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								Oid foreigntableid);
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreigntableid,
									   ForeignPath *best_path, List *tlist,
									   List *scan_clauses, Plan *outer_plan);
static void hdfsBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hdfsIterateForeignScan(ForeignScanState *node);
static void hdfsReScanForeignScan(ForeignScanState *node);
static void hdfsEndForeignScan(ForeignScanState *node);
static void hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es);
static bool hdfsAnalyzeForeignTable(Relation relation,
									AcquireSampleRowsFunc *func,
									BlockNumber *totalpages);
static void hdfsGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
									RelOptInfo *outerrel, RelOptInfo *innerrel,
									JoinType jointype,
									JoinPathExtraData *extra);
static bool hdfsRecheckForeignScan(ForeignScanState *node,
								   TupleTableSlot *slot);

static void hdfsGetForeignUpperPaths(PlannerInfo *root,
									 UpperRelationKind stage,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel,
									 void *extra);

/*
 * Helper functions
 */
static void prepare_query_params(PlanState *node,
								 List *fdw_exprs,
								 List **param_exprs,
								 Oid **param_types);
static void process_query_params(int index,
								 ExprContext *econtext,
								 List *param_exprs,
								 Oid *param_types);
static int	GetConnection(hdfs_opt *opt, Oid foreigntableid);

static bool hdfs_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
								 JoinType jointype, RelOptInfo *outerrel,
								 RelOptInfo *innerrel,
								 JoinPathExtraData *extra);

static bool hdfs_foreign_grouping_ok(PlannerInfo *root,
									 RelOptInfo *grouped_rel,
									 Node *havingQual);
static void hdfs_add_foreign_grouping_paths(PlannerInfo *root,
											RelOptInfo *input_rel,
											RelOptInfo *grouped_rel,
											GroupPathExtraData *extra);

#ifdef EDB_NATIVE_LANG
#define XACT_CB_SIGNATURE XactEvent event, void *arg
#else
#define XACT_CB_SIGNATURE XactEvent event, void *arg
#endif
static List *hdfs_build_scan_list_for_baserel(Oid relid, Index varno,
											  Bitmapset *attrs_used,
											  List **retrieved_attrs);
static void hdfs_build_whole_row_constr_info(hdfsFdwExecutionState *festate,
											 TupleDesc tupdesc,
											 Bitmapset *relids,
											 int max_relid,
											 List *whole_row_lists,
											 List *scan_tlist,
											 List *fdw_scan_tlist);
static HeapTuple hdfs_get_tuple_with_whole_row(hdfsFdwExecutionState *festate,
											   Datum *values, bool *nulls);
static HeapTuple hdfs_form_whole_row(hdfsWRState *wr_state, Datum *values,
									 bool *nulls);


static void hdfs_fdw_xact_callback(XACT_CB_SIGNATURE);
static List *hdfs_get_useful_ecs_for_relation(PlannerInfo *root,
											  RelOptInfo *rel);
static List *hdfs_get_useful_pathkeys_for_relation(PlannerInfo *root,
												   RelOptInfo *rel);
#if PG_VERSION_NUM >= 170000
static void hdfs_add_paths_with_pathkeys(PlannerInfo *root,
										 RelOptInfo *rel,
										 Path *epq_path,
										 Cost base_startup_cost,
										 Cost base_total_cost,
										 List *restrictlist);
#else
static void hdfs_add_paths_with_pathkeys(PlannerInfo *root,
										 RelOptInfo *rel,
										 Path *epq_path,
										 Cost base_startup_cost,
										 Cost base_total_cost);
#endif
static void hdfs_add_foreign_ordered_paths(PlannerInfo *root,
										   RelOptInfo *input_rel,
										   RelOptInfo *ordered_rel);
static void hdfs_add_foreign_final_paths(PlannerInfo *root,
										 RelOptInfo *input_rel,
										 RelOptInfo *final_rel,
										 FinalPathExtraData *extra);

#if PG_VERSION_NUM >= 160000
static TargetEntry *hdfs_tlist_member_match_var(Var *var, List *targetlist);
static List *hdfs_varlist_append_unique_var(List *varlist, Var *var);
#endif

Datum
hdfs_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

void
_PG_init(void)
{
	int			rc = 0;

	DefineCustomStringVariable("hdfs_fdw.classpath",
							   "Specify the path to HiveJdbcClient-X.X.jar, hadoop-common-X.X.X.jar and hive-jdbc-X.X.X-standalone.jar",
							   NULL,
							   &g_classpath,
							   "",
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("hdfs_fdw.jvmpath",
							   "Specify the path to libjvm.so",
							   NULL,
							   &g_jvmpath,
							   "",
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomBoolVariable("hdfs_fdw.enable_join_pushdown",
							 "enable/disable join pushdown",
							 NULL,
							 &enable_join_pushdown,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("hdfs_fdw.enable_aggregate_pushdown",
							 "Enable/Disable aggregate push down",
							 NULL,
							 &enable_aggregate_pushdown,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * ORDER BY is inefficient in Hive so it's disabled by default in
	 * hdfs_fdw.
	 */
	DefineCustomBoolVariable("hdfs_fdw.enable_order_by_pushdown",
							 "Enable/Disable ORDER BY push down",
							 NULL,
							 &enable_order_by_pushdown,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("hdfs_fdw.enable_limit_pushdown",
							 "Enable/Disable LIMIT/OFFSET push down",
							 NULL,
							 &enable_limit_pushdown,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	rc = Initialize();

	if (rc == -1)
	{
		/* TODO: The error hint is linux specific */
		ereport(ERROR,
				(errmsg("could not load JVM"),
				 errhint("Add path of libjvm.so to hdfs_fdw.jvmpath.")));
	}

	if (rc == -2)
		ereport(ERROR,
				(errmsg("class not found"),
				 errhint("Add path of HiveJdbcClient-X.X.jar to hdfs_fdw.classpath.")));

	if (rc < 0)
		ereport(ERROR,
				(errmsg("initialize failed with code %d", rc)));
}

void
_PG_fini(void)
{
	Destroy();
}

/*
 * hdfs_fdw_xact_callback --- cleanup at main-transaction end.
 */
static void
hdfs_fdw_xact_callback(XACT_CB_SIGNATURE)
{
	int			nestingLevel = 0;

	nestingLevel = DBCloseAllConnections();

	if (nestingLevel > 0)
		ereport(DEBUG1,
				(errmsg("hdfs_fdw: %d connection(s) closed", nestingLevel)));
}

/*
 * Foreign-data wrapper handler function, return the pointer of callback
 * functions pointers
 */
Datum
hdfs_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = hdfsGetForeignRelSize;
	routine->GetForeignPaths = hdfsGetForeignPaths;
	routine->GetForeignPlan = hdfsGetForeignPlan;
	routine->BeginForeignScan = hdfsBeginForeignScan;
	routine->IterateForeignScan = hdfsIterateForeignScan;
	routine->ReScanForeignScan = hdfsReScanForeignScan;
	routine->EndForeignScan = hdfsEndForeignScan;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = hdfsRecheckForeignScan;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = hdfsExplainForeignScan;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = hdfsAnalyzeForeignTable;

	/* Support functions for join push-down */
	routine->GetForeignJoinPaths = hdfsGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = hdfsGetForeignUpperPaths;

	RegisterXactCallback(hdfs_fdw_xact_callback, NULL);

	PG_RETURN_POINTER(routine);
}

/*
 * GetConnection
 * 		Create a connection to Hive/Spark server.
 */
static int
GetConnection(hdfs_opt *opt, Oid foreigntableid)
{
	Oid			userid = GetUserId();
	ForeignServer *server;
	ForeignTable *table;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	/* Connect to the server */
	return hdfs_get_connection(server, opt);
}

/*
 * hdfsGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
hdfsGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
					  Oid foreigntableid)
{
	HDFSFdwRelationInfo *fpinfo;
	ListCell   *lc;
	hdfs_opt   *options;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *database;
	const char *relname;
	const char *refname;

	/*
	 * We use HDFSFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (HDFSFdwRelationInfo *) palloc0(sizeof(HDFSFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be push down always. */
	fpinfo->pushdown_safe = true;

	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	hdfs_classify_conditions(root, baserel, baserel->baserestrictinfo,
							 &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);

	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Get the actual number of rows from server if use_remote_estimate is
	 * specified in options, if not, assume 1000.
	 */
	options = hdfs_get_options(foreigntableid);
	if (options->use_remote_estimate)
	{
		int			con_index;

		/* Connect to HIVE server */
		con_index = GetConnection(options, foreigntableid);

		baserel->rows = hdfs_rowcount(con_index, options, root,
									  baserel, fpinfo);

		hdfs_rel_connection(con_index);
	}
	else
		baserel->rows = 1000;

	fpinfo->rows = baserel->tuples = baserel->rows;

	/* Also store the options in fpinfo for further use */
	fpinfo->options = options;

	/* Set the flag enable_aggregate_pushdown of the base relation */
	fpinfo->enable_aggregate_pushdown = options->enable_aggregate_pushdown;

	/* Set the flag enable_order_by_pushdown of the base relation */
	fpinfo->enable_order_by_pushdown = options->enable_order_by_pushdown;
	fpinfo->client_type = options->client_type;

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output.  We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	database = options->dbname;
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(database),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}

/*
 * hdfsGetForeignPaths
 * 		Create possible scan paths for a scan on the foreign table.
 */
static void
hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) baserel->fdw_private;
	int			total_cost;
	ForeignPath *path;

	total_cost = fpinfo->fdw_startup_cost +
		fpinfo->fdw_tuple_cost * baserel->rows;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
#if PG_VERSION_NUM >= 180000
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   0,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   NIL, /* no fdw_restrictinfo list */
								   NIL);	/* no fdw_private data */
#elif PG_VERSION_NUM >= 170000
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   NIL, /* no fdw_restrictinfo list */
								   NIL);	/* no fdw_private data */
#else
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private data */
#endif

	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
#if PG_VERSION_NUM >= 170000
	hdfs_add_paths_with_pathkeys(root, baserel, NULL, fpinfo->fdw_startup_cost,
								 total_cost, NIL);
#else
	hdfs_add_paths_with_pathkeys(root, baserel, NULL, fpinfo->fdw_startup_cost,
								 total_cost);
#endif
}


/*
 * hdfsGetForeignPlan
 * 		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
hdfsGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *foreignrel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	List	   *fdw_scan_tlist = NIL;
	List	   *scan_var_list = NIL;
	List	   *whole_row_lists = NIL;
	bool		has_final_sort = false;
	bool		has_limit = false;

	/*
	 * Get FDW private data created by hdfsGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = intVal(list_nth(best_path->fdw_private,
										 FdwPathPrivateHasFinalSort));
		has_limit = intVal(list_nth(best_path->fdw_private,
									FdwPathPrivateHasLimit));
	}

	if (foreignrel->reloptkind == RELOPT_BASEREL ||
		foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		scan_relid = foreignrel->relid;
	else
	{
		scan_relid = 0;
		Assert(!scan_clauses);

		remote_conds = fpinfo->remote_conds;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by hdfs_classify_conditions are shown
	 * in fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we only strip the RestrictInfo nodes from the
	 * local_exprs list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (hdfs_is_foreign_expr(root, foreignrel, rinfo->clause, false))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	if (IS_JOIN_REL(foreignrel))
	{
		/* Build the list of columns to be fetched from the foreign server. */
		scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
										PVC_RECURSE_PLACEHOLDERS);
		scan_var_list = list_concat_unique(NIL, scan_var_list);

		scan_var_list = list_concat_unique(scan_var_list,
										   pull_var_clause((Node *) local_exprs,
														   PVC_RECURSE_PLACEHOLDERS));

		/*
		 * For join relations, planner needs targetlist, which represents the
		 * output of ForeignScan node. Prepare this before we modify
		 * scan_var_list to include Vars required by whole row references, if
		 * any.  Note that base foreign scan constructs the whole-row
		 * reference at the time of projection.  Joins are required to get
		 * them from the underlying base relations.  For a pushed down join
		 * the underlying relations do not exist, hence the whole-row
		 * references need to be constructed separately.
		 */
		fdw_scan_tlist = add_to_flat_tlist(NIL, scan_var_list);

		/*
		 * hive/spark does not allow row value constructors to be part of
		 * SELECT list.  Hence, whole row reference in join relations need to
		 * be constructed by combining all the attributes of required base
		 * relations into a tuple after fetching the result from the foreign
		 * server.  So adjust the targetlist to include all attributes for
		 * required base relations.  The function also returns list of Var
		 * node lists required to construct the whole-row references of the
		 * involved relations.
		 */
		scan_var_list = hdfs_adjust_whole_row_ref(root, scan_var_list,
												  &whole_row_lists,
												  foreignrel->relids);

		if (outer_plan)
		{
			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins.  Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}
		}
	}
	else if (IS_UPPER_REL(foreignrel))
	{
		/*
		 * scan_var_list should have expressions and not TargetEntry nodes.
		 * However grouped_tlist created has TLEs, thus retrieve them into
		 * scan_var_list.
		 */
		scan_var_list = list_concat_unique(NIL,
										   get_tlist_exprs(fpinfo->grouped_tlist,
														   false));

		/*
		 * The targetlist computed while assessing push-down safety represents
		 * the result we expect from the foreign server.
		 */
		fdw_scan_tlist = fpinfo->grouped_tlist;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	hdfs_deparse_select_stmt_for_rel(&sql, root, foreignrel, scan_var_list,
									 remote_conds, false,
									 best_path->path.pathkeys,
									 has_final_sort, has_limit,
									 &retrieved_attrs,
									 &params_list);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make2(makeString(sql.data),
							 retrieved_attrs);
	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

		/*
		 * To construct whole row references we need:
		 *
		 * 1. The lists of Var nodes required for whole-row references of
		 * joining relations
		 *
		 * 2. targetlist corresponding the result expected from the foreign
		 * server.
		 */
		if (whole_row_lists)
		{
			fdw_private = lappend(fdw_private, whole_row_lists);
			fdw_private = lappend(fdw_private,
								  add_to_flat_tlist(NIL, scan_var_list));
		}
	}

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private
							,fdw_scan_tlist
							,remote_exprs
							,outer_plan
		);
}

/*
 * hdfsBeginForeignScan
 * 		Create ForeignScan plan node which implements selected best path.
 */
static void
hdfsBeginForeignScan(ForeignScanState *node, int eflags)
{
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	hdfsFdwExecutionState *festate;
	RangeTblEntry *rte;
	hdfs_opt   *opt;
	EState	   *estate = node->ss.ps.state;
	int			rtindex;
	List	   *fdw_private = fsplan->fdw_private;

	festate = (hdfsFdwExecutionState *) palloc0(sizeof(hdfsFdwExecutionState));
	node->fdw_state = (void *) festate;

	/*
	 * If whole-row references are involved in pushed down join extract the
	 * information required to construct those.
	 */
	if (list_length(fdw_private) >= hdfsFdwPrivateScanTList)
	{
		List	   *whole_row_lists = list_nth(fdw_private,
											   hdfsFdwPrivateWholeRowLists);
		List	   *scan_tlist = list_nth(fdw_private,
										  hdfsFdwPrivateScanTList);
		TupleDesc	scan_tupdesc = ExecTypeFromTL(scan_tlist);

		hdfs_build_whole_row_constr_info(festate, tupleDescriptor,
										 fsplan->fs_relids,
										 list_length(node->ss.ps.state->es_range_table),
										 whole_row_lists, scan_tlist,
										 fsplan->fdw_scan_tlist);

		/* Change tuple descriptor to match the result from foreign server. */
		tupleDescriptor = scan_tupdesc;
	}

	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
#if PG_VERSION_NUM >= 160000
		rtindex = bms_next_member(fsplan->fs_base_relids, -1);
#else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
#endif

#if PG_VERSION_NUM >= 160000
	rte = exec_rt_fetch(rtindex, estate);
#else
	rte = rt_fetch(rtindex, estate->es_range_table);
#endif

	opt = hdfs_get_options(rte->relid);
	festate->con_index = GetConnection(opt, rte->relid);

	festate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "hdfs_fdw tuple data",
											   ALLOCSET_DEFAULT_SIZES);

	festate->query_executed = false;
	festate->query = strVal(list_nth(fdw_private, hdfsFdwScanPrivateSelectSql));
	festate->retrieved_attrs = (List *) list_nth(fdw_private,
												 hdfsFdwScanPrivateRetrievedAttrs);
	festate->rescan_count = 0;
	festate->attinmeta = TupleDescGetAttInMetadata(tupleDescriptor);

	/*
	 * Prepare remote query and also prepare for processing of parameters used
	 * in remote query, if any.
	 */
	hdfs_query_prepare(festate->con_index, opt, festate->query);

	festate->numParams = list_length(fsplan->fdw_exprs);
	if (festate->numParams > 0)
	{
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 &festate->param_exprs,
							 &festate->param_types);
	}
}

/*
 * hdfsIterateForeignScan
 * 		Retrieve next row from the result set and store in the tuple slot.
 */
static TupleTableSlot *
hdfsIterateForeignScan(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	RangeTblEntry *rte;
	int			rtindex;
	EState	   *estate = node->ss.ps.state;
	Datum	   *values;
	bool	   *nulls;
	int			natts;
	hdfs_opt   *options;
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MemoryContext oldcontext;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	AttInMetadata *attinmeta = festate->attinmeta;

	natts = attinmeta->tupdesc->natts;

	ExecClearTuple(slot);

	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
#if PG_VERSION_NUM >= 160000
		rtindex = bms_next_member(fsplan->fs_base_relids, -1);
#else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
#endif

	rte = rt_fetch(rtindex, estate->es_range_table);

	/* Get the options */
	options = hdfs_get_options(rte->relid);

	MemoryContextReset(festate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(festate->batch_cxt);

	values = (Datum *) palloc0(natts * sizeof(Datum));
	nulls = (bool *) palloc(natts * sizeof(bool));

	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, natts * sizeof(bool));

	if (!festate->query_executed)
	{
		/* Bind parameters */
		if (festate->numParams > 0)
			process_query_params(festate->con_index, econtext,
								 festate->param_exprs, festate->param_types);

		festate->query_executed = hdfs_execute_prepared(festate->con_index);
	}

	if (hdfs_fetch(festate->con_index) == 0)
	{
		HeapTuple	tuple;
		int			attid = 0;
		ListCell   *lc;

		foreach(lc, festate->retrieved_attrs)
		{
			bool		isnull = true;
			int			attnum = lfirst_int(lc) - 1;
			Oid			pgtype = TupleDescAttr(attinmeta->tupdesc, attnum)->atttypid;
			int32		pgtypmod = TupleDescAttr(attinmeta->tupdesc, attnum)->atttypmod;
			Datum		v;

			v = hdfs_get_value(festate->con_index, options, pgtype,
							   pgtypmod, attid, &isnull);
			if (!isnull)
			{
				nulls[attnum] = false;
				values[attnum] = v;
			}

			attid++;
		}

		if (list_length(fsplan->fdw_private) >= hdfsFdwPrivateScanTList)
		{
			/* Construct tuple with whole-row references. */
			tuple = hdfs_get_tuple_with_whole_row(festate, values, nulls);
		}
		else
		{
			/* Form the Tuple using Datums */
			tuple = heap_form_tuple(attinmeta->tupdesc, values, nulls);
		}

		ExecStoreHeapTuple(tuple, slot, true);
	}

	MemoryContextSwitchTo(oldcontext);

	return slot;
}

/*
 * hdfsReScanForeignScan
 * 		Restart the scan. Basically close the active resultset, and again
 * 		execute the remote query.
 */
static void
hdfsReScanForeignScan(ForeignScanState *node)
{
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;

	if (festate->query_executed)
	{
		hdfs_close_result_set(festate->con_index);
		festate->query_executed = false;
	}

	return;
}

/*
 * hdfsExplainForeignScan
 * 		Produce extra output for EXPLAIN of a ForeignScan on a foreign table.
 * 		This currently just adds "Remote SQL" followed by remote query for
 * 		'verbose' option.
 */
static void
hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List	   *fdw_private = fsplan->fdw_private;

	if (list_length(fdw_private) > hdfsFdwScanPrivateRelations)
	{
		char	   *relations = strVal(list_nth(fdw_private,
												hdfsFdwScanPrivateRelations));

		ExplainPropertyText("Relations", relations, es);
	}

	if (es->verbose)
	{
		char	   *sql;

		sql = strVal(list_nth(fdw_private, hdfsFdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

static int
hdfsAcquireSampleRowsFunc(Relation relation, int elevel,
						  HeapTuple *rows, int targrows,
						  double *totalrows,
						  double *totaldeadrows)
{
	/* TODO: Not Implemented */
	return 0;
}

/*
 * hdfsAnalyzeForeignTable
 * 		Test whether analyzing this foreign table is supported.
 */
static bool
hdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
						BlockNumber *totalpages)
{
	long		totalsize;
	hdfs_opt   *options;
	Oid			foreigntableid = RelationGetRelid(relation);
	int			con_index;

	*func = hdfsAcquireSampleRowsFunc;

	/* Get the options */
	options = hdfs_get_options(foreigntableid);

	/* Connect to HIVE server */
	con_index = GetConnection(options, foreigntableid);

	hdfs_analyze(con_index, options, relation);
	totalsize = hdfs_describe(con_index, options, relation);

	*totalpages = totalsize / BLCKSZ;
	return true;
}

/*
 * hdfsEndForeignScan
 * 		Finish scanning foreign table and dispose objects used for this scan.
 */
static void
hdfsEndForeignScan(ForeignScanState *node)
{
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;

	if (festate->query_executed)
	{
		hdfs_close_result_set(festate->con_index);
		festate->query_executed = false;
	}

	hdfs_rel_connection(festate->con_index);
	return;
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 List **param_exprs,
					 Oid **param_types)
{
	int			i;
	ListCell   *lc;
	Oid		   *paramTypes;
	int			numParams = list_length(fdw_exprs);

	Assert(numParams > 0);
	paramTypes = (Oid *) palloc0(sizeof(Oid) * numParams);

	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		paramTypes[i] = exprType(param_expr);
		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		i++;
	}

	*param_types = paramTypes;

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require fdw to know more than is desirable about
	 * Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);
}

static void
process_query_params(int con_index,
					 ExprContext *econtext,
					 List *param_exprs,
					 Oid *param_types)
{
	int			param_index;
	ListCell   *lc;

	param_index = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		hdfs_bind_var(con_index, param_index + 1, param_types[param_index],
					  expr_value, &isNull);
		param_index++;
	}
}

/*
 * hdfsGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
hdfsGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
						RelOptInfo *outerrel, RelOptInfo *innerrel,
						JoinType jointype, JoinPathExtraData *extra)
{
	HDFSFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	Cost		startup_cost;
	Cost		total_cost;
	HDFSFdwRelationInfo *fpinfo_o;
	HDFSFdwRelationInfo *fpinfo_i;

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	fpinfo_o = (HDFSFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (HDFSFdwRelationInfo *) innerrel->fdw_private;

	/* If join pushdown is not enabled, honor it. */
	if ((!IS_JOIN_REL(outerrel) && !fpinfo_o->options->enable_join_pushdown) ||
		(!IS_JOIN_REL(innerrel) && !fpinfo_i->options->enable_join_pushdown) ||
		!enable_join_pushdown)
		return;

	/*
	 * Create unfinished HDFSFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe.  Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (HDFSFdwRelationInfo *) palloc0(sizeof(HDFSFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	if (!hdfs_foreign_join_ok(root, joinrel, jointype, outerrel, innerrel,
							  extra))
		return;

	/* Set the flag enable_aggregate_pushdown of the join relation */
	fpinfo->enable_aggregate_pushdown =
		((HDFSFdwRelationInfo *) innerrel->fdw_private)->enable_aggregate_pushdown &&
		((HDFSFdwRelationInfo *) outerrel->fdw_private)->enable_aggregate_pushdown;

	/* Set the flag enable_order_by_pushdown of the join relation */
	fpinfo->enable_order_by_pushdown =
		((HDFSFdwRelationInfo *) innerrel->fdw_private)->enable_order_by_pushdown &&
		((HDFSFdwRelationInfo *) outerrel->fdw_private)->enable_order_by_pushdown;

	/*
	 * It is possible that two foreign servers are setup, one with 'hiveserver2'
	 * as the client_type and the other with 'spark'. More worse, it's possible
	 * that each of these servers are setup with a different host.
	 * That would not work, but the fdw does not complain either currently.
	 * This check does a partial risk mitigation.
	 */
	if (((HDFSFdwRelationInfo *) innerrel->fdw_private)->client_type !=
		((HDFSFdwRelationInfo *) outerrel->fdw_private)->client_type)
		elog(ERROR, "Multiple client_type server options not supported");

	fpinfo->client_type =
		((HDFSFdwRelationInfo *)innerrel->fdw_private)->client_type;

	/* TODO: Put accurate estimates here */
	startup_cost = 15.0;
	total_cost = 20 + startup_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
#if PG_VERSION_NUM >= 180000
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,
										joinrel->rows,
										0,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										NULL,
										extra->restrictlist,
										NIL);	/* no fdw_private */
#elif PG_VERSION_NUM >= 170000
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,
										joinrel->rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										NULL,
										extra->restrictlist,
										NIL);	/* no fdw_private */
#else
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,
										joinrel->rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										NULL,
										NIL);	/* no fdw_private */
#endif

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	/* Add paths with pathkeys */
#if PG_VERSION_NUM >= 170000
	hdfs_add_paths_with_pathkeys(root, joinrel, NULL, startup_cost,
								 total_cost, extra->restrictlist);
#else
	hdfs_add_paths_with_pathkeys(root, joinrel, NULL, startup_cost,
								 total_cost);
#endif

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * hdfs_foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be
 * 		pushed down to the foreign server.  As a side effect, save
 * 		information we obtain in this function to HDFSFdwRelationInfo
 * 		passed in.
 */
static bool
hdfs_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
					 JoinType jointype, RelOptInfo *outerrel,
					 RelOptInfo *innerrel, JoinPathExtraData *extra)
{
	HDFSFdwRelationInfo *fpinfo;
	HDFSFdwRelationInfo *fpinfo_o;
	HDFSFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;

	/* We support pushing down INNER, LEFT, RIGHT and FULL joins. */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL)
		return false;

	fpinfo = (HDFSFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (HDFSFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (HDFSFdwRelationInfo *) innerrel->fdw_private;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations.  Hence the join
	 * can not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = hdfs_is_foreign_expr(root, joinrel,
															rinfo->clause, true);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
			{
				/*
				 * Unlike postgres_fdw, don't append the join clauses to
				 * remote_conds, instead keep the join clauses separate.
				 * Currently, we are providing limited operator pushability
				 * support for join pushdown, hence we keep those clauses
				 * separate to avoid INNER JOIN not getting pushdown if any of
				 * the WHERE clause is not shippable as per join pushdown
				 * shippability.
				 */
				if (jointype == JOIN_INNER)
					joinclauses = lappend(joinclauses, rinfo);
				else
					fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			}
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * hdfs_deparse_explicit_target_list() isn't smart enough to handle
	 * anything other than a Var.  In particular, if there's some
	 * PlaceHolderVar that would need to be evaluated within this join tree
	 * (because there's an upper reference to a quantity that may go to NULL
	 * as a result of an outer join), then we can't try to push the join down
	 * because we'll fail when we get to hdfs_deparse_explicit_target_list().
	 * However, a PlaceHolderVar that needs to be evaluated *at the top* of
	 * this join tree is OK, because we can do that locally after fetching the
	 * results from the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
	fpinfo->lower_subquery_rels = bms_union(fpinfo_o->lower_subquery_rels,
											fpinfo_i->lower_subquery_rels);

	/*
	 * In case innerrel is a joinrel, HiveSQL (as of hive version 3.1.2) does
	 * not support syntax of the form "A JOIN (B JOIN C)". It expects the
	 * right side to be a subquery instead. However, if the outerrel is a
	 * joinrel e.g. "(A JOIN B) JOIN C", this syntax is supported, so here we
	 * decide to deparse only the innerrels that are joinrels to subquery.
	 */
	if (IS_JOIN_REL(innerrel))
	{
		fpinfo->make_innerrel_subquery = true;
		fpinfo->lower_subquery_rels =
			bms_add_members(fpinfo->lower_subquery_rels,
							innerrel->relids);
	}

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation. This
	 * avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses.  For an OUTER join, the clauses from the outer
	 * side are added to remote_conds since those can be evaluated after the
	 * join is evaluated.  The clauses from inner side are added to the
	 * joinclauses, since they need to evaluated while constructing the join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:

			/*
			 * In case innerrel is a joinrel, we will be forming the subquery
			 * as indicated above, hence we do not need to add innerrel's
			 * remote conditions to fpinfo remote conditions.
			 */
			if (!fpinfo->make_innerrel_subquery)
				fpinfo->remote_conds = hdfs_list_concat(fpinfo->remote_conds,
														fpinfo_i->remote_conds);

			fpinfo->remote_conds = hdfs_list_concat(fpinfo->remote_conds,
													fpinfo_o->remote_conds);
			break;

		case JOIN_LEFT:
			/* Check that clauses from the inner side are pushable or not. */
			foreach(lc, fpinfo_i->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!hdfs_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			if (!fpinfo->make_innerrel_subquery)
				fpinfo->joinclauses = hdfs_list_concat(fpinfo->joinclauses,
													   fpinfo_i->remote_conds);

			fpinfo->remote_conds = hdfs_list_concat(fpinfo->remote_conds,
													fpinfo_o->remote_conds);
			break;

		case JOIN_RIGHT:
			/* Check that clauses from the outer side are pushable or not. */
			foreach(lc, fpinfo_o->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!hdfs_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			fpinfo->joinclauses = hdfs_list_concat(fpinfo->joinclauses,
												   fpinfo_o->remote_conds);
			if (!fpinfo->make_innerrel_subquery)
				fpinfo->remote_conds = hdfs_list_concat(fpinfo->remote_conds,
														fpinfo_i->remote_conds);
			break;

		case JOIN_FULL:

			/*
			 * In this case, if any of the input relations has conditions, we
			 * need to deparse that relation as a subquery so that the
			 * conditions can be evaluated before the join.  Remember it in
			 * the fpinfo of this relation so that the deparser can take
			 * appropriate action.  Also, save the relids of base relations
			 * covered by that relation for later use by the deparser.
			 */
			if (fpinfo_o->remote_conds)
			{
				fpinfo->make_outerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									outerrel->relids);
			}

			/*
			 * In case innerrel is a joinrel we have already set the subquery
			 * flag and added the relids to subquery rel list.
			 */
			if (fpinfo_i->remote_conds && !fpinfo->make_innerrel_subquery)
			{
				fpinfo->make_innerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									innerrel->relids);
			}

			break;

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "(%s) %s JOIN (%s)",
					 fpinfo_o->relation_name->data,
					 hdfs_get_jointype_name(fpinfo->jointype),
					 fpinfo_i->relation_name->data);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fpinfo->relation_index == 0);	/* shouldn't be set yet */
	fpinfo->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

/*
 * hdfsRecheckForeignScan
 *		Execute a local join execution plan for a foreign join
 */
static bool
hdfsRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
	Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	PlanState  *outerPlan = outerPlanState(node);
	TupleTableSlot *result;

	/* For base foreign relations, it suffices to set fdw_recheck_quals */
	if (scanrelid > 0)
		return true;

	Assert(outerPlan != NULL);

	/* Execute a local join execution plan */
	result = ExecProcNode(outerPlan);
	if (TupIsNull(result))
		return false;

	/* Store result in the given slot */
	ExecCopySlot(slot, result);

	return true;
}

/*
 * hdfs_adjust_whole_row_ref
 * 		If the given list of Var nodes has whole-row reference, add Var
 * 		nodes corresponding to all the attributes of the corresponding
 * 		base relation.
 *
 * The function also returns an array of lists of var nodes.  The array is
 * indexed by the RTI and entry there contains the list of Var nodes which
 * make up the whole-row reference for corresponding base relation.
 * The relations not covered by given join and the relations which do not
 * have whole-row references will have NIL entries.
 *
 * If there are no whole-row references in the given list, the given list is
 * returned unmodified and the other list is NIL.
 */
List *
hdfs_adjust_whole_row_ref(PlannerInfo *root, List *scan_var_list,
						  List **whole_row_lists, Bitmapset *relids)
{
	ListCell   *lc;
	bool		has_whole_row = false;
	List	  **wr_list_array = NULL;
	int			cnt_rt;
	List	   *wr_scan_var_list = NIL;

	*whole_row_lists = NIL;

	/* Check if there exists at least one whole row reference. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		if (var->varattno == 0)
		{
			has_whole_row = true;
			break;
		}
	}

	if (!has_whole_row)
		return scan_var_list;

	/*
	 * Allocate large enough memory to hold whole-row Var lists for all the
	 * relations. This array will then be converted into a list of lists.
	 * Since all the base relations are marked by range table index, it's easy
	 * to keep track of the ones whose whole-row references have been taken
	 * care of.
	 */
	wr_list_array = (List **) palloc0(sizeof(List *) *
									  list_length(root->parse->rtable));

	/* Adjust the whole-row references as described in the prologue. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);
#if PG_VERSION_NUM >= 160000
		ListCell   *cell;
#endif

		Assert(IsA(var, Var));

		if (var->varattno == 0 && !wr_list_array[var->varno - 1])
		{
			List	   *wr_var_list;
			List	   *retrieved_attrs;
			RangeTblEntry *rte = rt_fetch(var->varno, root->parse->rtable);
			Bitmapset  *attrs_used;

			Assert(OidIsValid(rte->relid));

			/*
			 * Get list of Var nodes for all undropped attributes of the base
			 * relation.
			 */
			attrs_used = bms_make_singleton(0 -
											FirstLowInvalidHeapAttributeNumber);

			/*
			 * If the whole-row reference falls on the nullable side of the
			 * outer join and that side is null in a given result row, the
			 * whole row reference should be set to NULL.  In this case, all
			 * the columns of that relation will be NULL, but that does not
			 * help since those columns can be genuinely NULL in a row.
			 */
			wr_var_list =
				hdfs_build_scan_list_for_baserel(rte->relid, var->varno,
												 attrs_used,
												 &retrieved_attrs);
			wr_list_array[var->varno - 1] = wr_var_list;
#if PG_VERSION_NUM >= 160000
			foreach(cell, wr_var_list)
			{
				Var		   *tlvar = (Var *) lfirst(cell);

				wr_scan_var_list = hdfs_varlist_append_unique_var(wr_scan_var_list,
										  tlvar);
			}
#else
			wr_scan_var_list = list_concat_unique(wr_scan_var_list,
												  wr_var_list);
#endif
			bms_free(attrs_used);
			list_free(retrieved_attrs);
		}
		else
#if PG_VERSION_NUM >= 160000
		wr_scan_var_list = hdfs_varlist_append_unique_var(wr_scan_var_list, var);
#else
		wr_scan_var_list = list_append_unique(wr_scan_var_list, var);
#endif
	}

	/*
	 * Collect the required Var node lists into a list of lists ordered by the
	 * base relations' range table indexes.
	 */
	cnt_rt = -1;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
		*whole_row_lists = lappend(*whole_row_lists, wr_list_array[cnt_rt - 1]);

	pfree(wr_list_array);
	return wr_scan_var_list;
}

/*
 * hdfs_build_scan_list_for_baserel
 * 		Build list of nodes corresponding to the attributes requested for
 * 		given base relation.
 *
 * The list contains Var nodes corresponding to the attributes specified in
 * attrs_used. If whole-row reference is required, the functions adds Var
 * nodes corresponding to all the attributes in the relation.
 */
static List *
hdfs_build_scan_list_for_baserel(Oid relid, Index varno,
								 Bitmapset *attrs_used,
								 List **retrieved_attrs)
{
	int			attno;
	List	   *tlist = NIL;
	Node	   *node;
	bool		wholerow_requested = false;
	Relation	relation;
	TupleDesc	tupdesc;

	Assert(OidIsValid(relid));

	*retrieved_attrs = NIL;

	/* Planner must have taken a lock, so request no lock here */
	relation = table_open(relid, NoLock);

	tupdesc = RelationGetDescr(relation);

	/* Is whole-row reference requested? */
	wholerow_requested = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
									   attrs_used);

	/* Handle user defined attributes first. */
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/*
		 * For a required attribute create a Var node and add corresponding
		 * attribute number to the retrieved_attrs list.
		 */
		if (wholerow_requested ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			node = (Node *) makeVar(varno, attno, attr->atttypid,
									attr->atttypmod, attr->attcollation, 0);
			tlist = lappend(tlist, node);

			*retrieved_attrs = lappend_int(*retrieved_attrs, attno);
		}
	}

	table_close(relation, NoLock);

	return tlist;
}

/*
 * hdfs_build_whole_row_constr_info
 *		Calculate and save the information required to construct whole row
 *		references of base foreign relations involved in the pushed down join.
 *
 * tupdesc is the tuple descriptor describing the result returned by the
 * ForeignScan node.  It is expected to be same as
 * ForeignScanState::ss::ss_ScanTupleSlot, which is constructed using
 * fdw_scan_tlist.
 *
 * relids is the the set of relations participating in the pushed down join.
 *
 * max_relid is the maximum number of relation index expected.
 *
 * whole_row_lists is the list of Var node lists constituting the whole-row
 * reference for base relations in the relids in the same order.
 *
 * scan_tlist is the targetlist representing the result fetched from the
 * foreign server.
 *
 * fdw_scan_tlist is the targetlist representing the result returned by the
 * ForeignScan node.
 */
static void
hdfs_build_whole_row_constr_info(hdfsFdwExecutionState *festate,
								 TupleDesc tupdesc, Bitmapset *relids,
								 int max_relid, List *whole_row_lists,
								 List *scan_tlist, List *fdw_scan_tlist)
{
	int			cnt_rt;
	int			cnt_vl;
	int			cnt_attr;
	ListCell   *lc;
	int		   *fs_attr_pos = NULL;
	hdfsWRState **hdfswrstates = NULL;
	int			fs_num_atts;

	/*
	 * Allocate memory to hold whole-row reference state for each relation.
	 * Indexing by the range table index is faster than maintaining an
	 * associative map.
	 */
	hdfswrstates = (hdfsWRState **) palloc0(sizeof(hdfsWRState *) * max_relid);

	/*
	 * Set the whole-row reference state for the relations whose whole-row
	 * reference needs to be constructed.
	 */
	cnt_rt = -1;
	cnt_vl = 0;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
	{
		hdfsWRState *wr_state = (hdfsWRState *) palloc0(sizeof(hdfsWRState));
		List	   *var_list = list_nth(whole_row_lists, cnt_vl++);
		int			natts;

		/* Skip the relations without whole-row references. */
		if (list_length(var_list) <= 0)
			continue;

		natts = list_length(var_list);
		wr_state->attr_pos = (int *) palloc(sizeof(int) * natts);

		/*
		 * Create a map of attributes required for whole-row reference to
		 * their positions in the result fetched from the foreign server.
		 */
		cnt_attr = 0;
		foreach(lc, var_list)
		{
			Var		   *var = lfirst(lc);
			TargetEntry *tle_sl;

			Assert(IsA(var, Var) && var->varno == cnt_rt);

#if PG_VERSION_NUM >= 160000
			tle_sl = hdfs_tlist_member_match_var(var, scan_tlist);
#else
			tle_sl = tlist_member((Expr *) var, scan_tlist);
#endif
			Assert(tle_sl);

			wr_state->attr_pos[cnt_attr++] = tle_sl->resno - 1;
		}
		Assert(natts == cnt_attr);

		/* Build rest of the state */
		wr_state->tupdesc = ExecTypeFromExprList(var_list);
		Assert(natts == wr_state->tupdesc->natts);
		wr_state->values = (Datum *) palloc(sizeof(Datum) * natts);
		wr_state->nulls = (bool *) palloc(sizeof(bool) * natts);
		BlessTupleDesc(wr_state->tupdesc);
		hdfswrstates[cnt_rt - 1] = wr_state;
	}

	/*
	 * Construct the array mapping columns in the ForeignScan node output to
	 * their positions in the result fetched from the foreign server. Positive
	 * values indicate the locations in the result and negative values
	 * indicate the range table indexes of the base table whose whole-row
	 * reference values are requested in that place.
	 */
	fs_num_atts = list_length(fdw_scan_tlist);
	fs_attr_pos = (int *) palloc(sizeof(int) * fs_num_atts);
	cnt_attr = 0;
	foreach(lc, fdw_scan_tlist)
	{
		TargetEntry *tle_fsl = lfirst(lc);
		Var		   *var = (Var *) tle_fsl->expr;

		Assert(IsA(var, Var));
		if (var->varattno == 0)
			fs_attr_pos[cnt_attr] = -var->varno;
		else
		{

#if PG_VERSION_NUM >= 160000
			TargetEntry *tle_sl = hdfs_tlist_member_match_var(var, scan_tlist);
#else
			TargetEntry *tle_sl = tlist_member((Expr *) var, scan_tlist);
#endif

			Assert(tle_sl);
			fs_attr_pos[cnt_attr] = tle_sl->resno - 1;
		}
		cnt_attr++;
	}

	/*
	 * The tuple descriptor passed in should have same number of attributes as
	 * the entries in fdw_scan_tlist.
	 */
	Assert(fs_num_atts == tupdesc->natts);

	festate->hdfswrstates = hdfswrstates;
	festate->wr_attrs_pos = fs_attr_pos;
	festate->wr_tupdesc = tupdesc;
	festate->wr_values = (Datum *) palloc(sizeof(Datum) * tupdesc->natts);
	festate->wr_nulls = (bool *) palloc(sizeof(bool) * tupdesc->natts);

	return;
}

/*
 * hdfs_get_tuple_with_whole_row
 *		Construct the result row with whole-row references.
 */
static HeapTuple
hdfs_get_tuple_with_whole_row(hdfsFdwExecutionState *festate, Datum *values,
							  bool *nulls)
{
	TupleDesc	tupdesc = festate->wr_tupdesc;
	Datum	   *wr_values = festate->wr_values;
	bool	   *wr_nulls = festate->wr_nulls;
	int			cnt_attr;
	HeapTuple	tuple = NULL;

	for (cnt_attr = 0; cnt_attr < tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = festate->wr_attrs_pos[cnt_attr];

		if (attr_pos >= 0)
		{
			wr_values[cnt_attr] = values[attr_pos];
			wr_nulls[cnt_attr] = nulls[attr_pos];
		}
		else
		{
			/*
			 * The RTI of relation whose whole row reference is to be
			 * constructed is stored as -ve attr_pos.
			 */
			hdfsWRState *wr_state = festate->hdfswrstates[-attr_pos - 1];

			wr_nulls[cnt_attr] = nulls[wr_state->wr_null_ind_pos];
			if (!wr_nulls[cnt_attr])
			{
				HeapTuple	wr_tuple = hdfs_form_whole_row(wr_state,
														   values,
														   nulls);

				wr_values[cnt_attr] = HeapTupleGetDatum(wr_tuple);
			}
		}
	}

	tuple = heap_form_tuple(tupdesc, wr_values, wr_nulls);
	return tuple;
}

/*
 * hdfs_form_whole_row
 * 		The function constructs whole-row reference for a base relation
 * 		with the information given in wr_state.
 *
 * wr_state contains the information about which attributes from values and
 * nulls are to be used and in which order to construct the whole-row
 * reference.
 */
static HeapTuple
hdfs_form_whole_row(hdfsWRState *wr_state, Datum *values, bool *nulls)
{
	int			cnt_attr;

	for (cnt_attr = 0; cnt_attr < wr_state->tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = wr_state->attr_pos[cnt_attr];

		wr_state->values[cnt_attr] = values[attr_pos];
		wr_state->nulls[cnt_attr] = nulls[attr_pos];
	}
	return heap_form_tuple(wr_state->tupdesc, wr_state->values,
						   wr_state->nulls);
}

/*
 * hdfs_foreign_grouping_ok
 * 		Assess whether the aggregation, grouping and having operations can
 * 		be pushed down to the foreign server.  As a side effect, save
 * 		information we obtain in this function to HDFSFdwRelationInfo of
 * 		the input relation.
 */
static bool
hdfs_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
						 Node *havingQual)
{
	Query	   *query = root->parse;
	PathTarget *grouping_target = grouped_rel->reltarget;
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) grouped_rel->fdw_private;
	HDFSFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (HDFSFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underneath input relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Evaluate grouping targets and check whether they are safe to push down
	 * to the foreign side.  All GROUP BY expressions will be part of the
	 * grouping target and thus there is no need to evaluate it separately.
	 * While doing so, add required expressions into target list which can
	 * then be used to pass to foreign server.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any of the GROUP BY expression is not shippable we can not
			 * push down aggregation to the foreign server.
			 */
			if (!hdfs_is_foreign_expr(root, grouped_rel, expr, true))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (hdfs_is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/* Check entire expression whether it is pushable or not */
			if (hdfs_is_foreign_expr(root, grouped_rel, expr, true) &&
				!hdfs_is_foreign_param(root, grouped_rel, expr))
			{
				/* Pushable, add to tlist */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				List	   *aggvars;

				/* Not matched exactly, pull the var with aggregates then */
				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!hdfs_is_foreign_expr(root, grouped_rel, (Expr *) aggvars, true))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain var
				 * nodes should be either same as some GROUP BY expression or
				 * part of some GROUP BY expression. In later case, the query
				 * cannot refer plain var nodes without the surrounding
				 * expression.  In both the cases, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * adding pulled plain var nodes in SELECT clause will cause
				 * an error on the foreign server if they are not same as some
				 * GROUP BY expression.
				 */
				foreach(l, aggvars)
				{
					Expr	   *aggref = (Expr *) lfirst(l);

					if (IsA(aggref, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(aggref));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable having clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
#if PG_VERSION_NUM >= 160000
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#elif PG_VERSION_NUM >= 140000
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#else
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#endif

			if (!hdfs_is_foreign_expr(root, grouped_rel, expr, true))
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
			else
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!hdfs_is_foreign_expr(root, grouped_rel, expr, true))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
					 ofpinfo->relation_name->data);

	return true;
}

/*
 * hdfsGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
hdfsGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						 RelOptInfo *input_rel, RelOptInfo *output_rel,
						 void *extra)
{
	HDFSFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((HDFSFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (HDFSFdwRelationInfo *) palloc0(sizeof(HDFSFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			hdfs_add_foreign_grouping_paths(root, input_rel, output_rel,
											(GroupPathExtraData *) extra);
			break;
		case UPPERREL_ORDERED:
			hdfs_add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			hdfs_add_foreign_final_paths(root, input_rel, output_rel,
										 (FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * hdfs_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
hdfs_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								RelOptInfo *grouped_rel,
								GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	HDFSFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	Cost		startup_cost;
	Cost		total_cost;
	double		num_groups;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/* Set aggregation flag of aggregate relation */
	fpinfo->enable_aggregate_pushdown =
		((HDFSFdwRelationInfo *) input_rel->fdw_private)->enable_aggregate_pushdown;

	/* If aggregate pushdown is not enabled, honor it. */
	if (!enable_aggregate_pushdown || !fpinfo->enable_aggregate_pushdown)
		return;

	/* Assess if it is safe to push down aggregation and grouping. */
	if (!hdfs_foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	fpinfo->enable_order_by_pushdown =
		((HDFSFdwRelationInfo *) input_rel->fdw_private)->enable_order_by_pushdown;

	fpinfo->client_type =
		((HDFSFdwRelationInfo *) input_rel->fdw_private)->client_type;

	/*
	 * TODO: Put accurate estimates here.
	 *
	 * Cost used here is minimum of the cost estimated for base and join
	 * relation.
	 */
	startup_cost = 15;
	total_cost = 10 + startup_cost;

	/* Estimate output tuples which should be same as number of groups */
#if PG_VERSION_NUM >= 140000
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL, NULL);
#else
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL);
#endif

	/* Create and add foreign path to the grouping relation. */
#if PG_VERSION_NUM >= 180000
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  0,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL,	/* no fdw_restrictinfo list */
										  NIL); /* no fdw_private */
#elif PG_VERSION_NUM >= 170000
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL,	/* no fdw_restrictinfo list */
										  NIL); /* no fdw_private */
#else
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * hdfs_get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
hdfs_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * hdfs_get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
hdfs_get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 */
			if (!hdfs_is_foreign_pathkey(root, rel, pathkey))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = hdfs_get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceMember *em = NULL;
		EquivalenceClass *cur_ec = lfirst(lc);
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		em = hdfs_find_em_for_rel(root, cur_ec, rel);

		/* Can't push down the sort if the EC's opfamily is not shippable. */
		if (!hdfs_is_builtin(linitial_oid(cur_ec->ec_opfamilies)))
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
#if PG_VERSION_NUM >= 180000
										 COMPARE_LT,
#else
										 BTLessStrategyNumber,
#endif
										 false);

		/* Check for sort operator pushability. */
		if (hdfs_get_sortby_direction_string(em, pathkey) == NULL)
			continue;

		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * hdfs_add_paths_with_pathkeys
 *		 Add path with root->query_pathkeys if that's pushable.
 *
 * Pushing down query_pathkeys to the foreign server might let us avoid a
 * local sort.
 */
#if PG_VERSION_NUM >= 170000
static void
hdfs_add_paths_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
							 Path *epq_path, Cost base_startup_cost,
							 Cost base_total_cost, List *restrictlist)
#else
static void
hdfs_add_paths_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
							 Path *epq_path, Cost base_startup_cost,
							 Cost base_total_cost)
#endif
{
	ListCell   *lc;
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */

	if (!enable_order_by_pushdown ||
		!((HDFSFdwRelationInfo *) rel->fdw_private)->enable_order_by_pushdown)
		return;

	useful_pathkeys_list = hdfs_get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		/* TODO put accurate estimates. */
		startup_cost = base_startup_cost * DEFAULT_HDFS_SORT_MULTIPLIER;
		total_cost = base_total_cost * DEFAULT_HDFS_SORT_MULTIPLIER;

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
#if PG_VERSION_NUM >= 180000
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rel->rows,
											 0,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL,	/* no fdw_restrictinfo list */
											 NIL));	/* no fdw_private list */
#elif PG_VERSION_NUM >= 170000
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rel->rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL,	/* no fdw_restrictinfo list */
											 NIL));	/* no fdw_private list */
#else
		add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rel->rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL));	/* no fdw_private list */
#endif
		else
#if PG_VERSION_NUM >= 180000
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rel->rows,
											  0,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  restrictlist,
											  NIL));	/* no fdw_private */
#elif PG_VERSION_NUM >= 170000
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rel->rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  restrictlist,
											  NIL));	/* no fdw_private */
#else
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rel->rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  NIL));	/* no fdw_private */
#endif
	}
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
hdfs_find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			hdfs_is_foreign_expr(root, rel, em->em_expr, true))
			return em;
	}

	return NULL;
}

/*
 * hdfs_add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
hdfs_add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
							   RelOptInfo *ordered_rel)
{
	Query	   *parse = root->parse;
	HDFSFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	HDFSFdwRelationInfo *fpinfo = ordered_rel->fdw_private;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *ordered_path;
	ListCell   *lc;

	/* Set the flag enable_order_by_pushdown of the ordered relation */
	fpinfo->enable_order_by_pushdown =
		((HDFSFdwRelationInfo *) input_rel->fdw_private)->enable_order_by_pushdown;

	if (!enable_order_by_pushdown || !fpinfo->enable_order_by_pushdown)
		return;

	fpinfo->client_type =
		((HDFSFdwRelationInfo *) input_rel->fdw_private)->client_type;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL ||
		input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach(lc, root->sort_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		EquivalenceMember *em;

		/*
		 * hdfs_is_foreign_expr would detect volatile expressions as well, but
		 * checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/*
		 * The EC must contain a shippable EM that is computed in input_rel's
		 * reltarget, else we can't push down the sort.
		 */
		em = hdfs_find_em_for_rel_target(root, pathkey_ec, input_rel);

		if (hdfs_get_sortby_direction_string(em, pathkey) == NULL)
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* TODO: Put accurate estimates */
	startup_cost = 15;
	total_cost = 10 + startup_cost;
	rows = 10;

	/*
	 * Build the fdw_private list that will be used by hdfsGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(true), makeInteger(false));

	/* Create foreign ordering path */
#if PG_VERSION_NUM >= 180000
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 0,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 NIL,	/* no fdw_restrictinfo list */
											 fdw_private);
#elif PG_VERSION_NUM >= 170000
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 NIL,	/* no fdw_restrictinfo list */
											 fdw_private);
#else
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 fdw_private);
#endif

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}

/*
 * hdfs_find_em_for_rel_target
 *
 * Find an EquivalenceClass member that is to be computed as a sort column
 * in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
hdfs_find_em_for_rel_target(PlannerInfo *root, EquivalenceClass *ec,
							RelOptInfo *rel)
{
	PathTarget *target = rel->reltarget;
	ListCell   *lc1;
	int			i;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (!equal(em_expr, expr))
				continue;

			/* Check that expression (including relabels!) is shippable */
			if (hdfs_is_foreign_expr(root, rel, em->em_expr, true))
				return em;
		}

		i++;
	}

	return NULL;
}

/*
 * hdfs_add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
hdfs_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
							 RelOptInfo *final_rel, FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	HDFSFdwRelationInfo *ifpinfo = (HDFSFdwRelationInfo *) input_rel->fdw_private;
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	if (!enable_limit_pushdown)
		return;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* HiveQL does not support only OFFSET clause in a SELECT command. */
	if (ifpinfo->client_type == HIVESERVER2 &&
		parse->limitOffset && !parse->limitCount)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
#if PG_VERSION_NUM >= 180000
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   0,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NIL,		/* no fdw_restrictinfo list */
													   NIL);	/* no fdw_private */
#elif PG_VERSION_NUM >= 170000
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NIL,		/* no fdw_restrictinfo list */
													   NIL);	/* no fdw_private */
#else
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NIL);	/* no fdw_private */
#endif

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		/* Do not push down LIMIT if ORDER BY push down is disabled */
		if (!enable_order_by_pushdown)
			return;

		input_rel = ifpinfo->outerrel;
		ifpinfo = (HDFSFdwRelationInfo *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * hdfs doesn't support LIMIT/OFFSET NULL/ALL syntax, however, only check
	 * for LIMIT NULL/ALL because one can specify a valid LIMIT value with
	 * OFFSET NULL.  This can be handled by checking for OFFSET NULL during
	 * deparse and ignoring it.  If const LIMIT node is null then do not
	 * pushdown limit/offset clause.
	 */
	if (parse->limitCount)
	{
		Node	   *node = parse->limitCount;

		if (nodeTag(node) != T_Const &&
			nodeTag(node) != T_Param)
			return;

		/* Negative LIMIT value is not supported in Hive/Spark */
		if (nodeTag(node) == T_Const)
		{
			if (((Const *) node)->constisnull ||
				(((Const *) node)->consttype != INT8OID) ||
				(DatumGetInt64(((Const *) node)->constvalue) < 0))
				return;
		}
	}
	if (parse->limitOffset)
	{
		Node	   *node = parse->limitOffset;

		if (nodeTag(node) != T_Const &&
			nodeTag(node) != T_Param)
			return;

		/* Negative OFFSET value is not supported in Hive/Spark */
		if (nodeTag(node) == T_Const)
		{
			if (!((Const *) node)->constisnull &&
				((((Const *) node)->consttype != INT8OID) ||
				(DatumGetInt64(((Const *) node)->constvalue) < 0)))
				return;
		}
	}

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!hdfs_is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset, true) ||
		!hdfs_is_foreign_expr(root, input_rel, (Expr *) parse->limitCount, true))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* TODO: Put accurate estimates */
	startup_cost = 1;
	total_cost = 1 + startup_cost;
	rows = 1;

	/*
	 * Build the fdw_private list that will be used by hdfsGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
#if PG_VERSION_NUM >= 180000
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   0,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   NIL,		/* no fdw_restrictinfo list */
										   fdw_private);
#elif PG_VERSION_NUM >= 170000
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   NIL,		/* no fdw_restrictinfo list */
										   fdw_private);
#else
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   fdw_private);
#endif

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

/*
 * hdfs_get_sortby_direction_string
 *		Fetch the operator oid from the operator family and datatype, and check
 *		whether the operator is the default for sort expr's datatype. If it is,
 *		then return ASC or DESC accordingly; NULL otherwise.
 */
char *
hdfs_get_sortby_direction_string(EquivalenceMember *em, PathKey *pathkey)
{
	Oid			oprid;
	TypeCacheEntry *typentry;

	if (em == NULL)
		return NULL;

	/* Can't push down the sort if pathkey's opfamily is not shippable. */
	if (!hdfs_is_builtin(pathkey->pk_opfamily))
		return NULL;

#if PG_VERSION_NUM >= 180000
	oprid = get_opfamily_member_for_cmptype(pathkey->pk_opfamily,
											em->em_datatype,
											em->em_datatype,
											pathkey->pk_cmptype);
#else
	oprid = get_opfamily_member(pathkey->pk_opfamily, em->em_datatype,
								em->em_datatype, pathkey->pk_strategy);
#endif

	if (!OidIsValid(oprid))
		elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
#if PG_VERSION_NUM >= 180000
			 pathkey->pk_cmptype, em->em_datatype, em->em_datatype,
#else
			 pathkey->pk_strategy, em->em_datatype, em->em_datatype,
#endif
			 pathkey->pk_opfamily);

	/* Can't push down the sort if the operator is not shippable. */
	if (!hdfs_is_builtin(oprid))
		return NULL;

	/*
	 * See whether the operator is default < or > for sort expr's datatype.
	 * Here we need to use the expression's actual type to discover whether
	 * the desired operator will be the default or not.
	 */
	typentry = lookup_type_cache(exprType((Node *) em->em_expr),
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (oprid == typentry->lt_opr)
		return "ASC";
	else if (oprid == typentry->gt_opr)
		return "DESC";

	return NULL;
}

#if PG_VERSION_NUM >= 160000
/*
 * hdfs_tlist_member_match_var
 *	  Finds the (first) member of the given tlist whose Var is
 *	  same as the given Var.  Result is NULL if no such member.
 */
static TargetEntry *
hdfs_tlist_member_match_var(Var *var, List *targetlist)
{
	ListCell   *temp;

	foreach(temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);
		Var		   *tlvar = (Var *) tlentry->expr;

		if (!tlvar || !IsA(tlvar, Var))
			continue;
		if (var->varno == tlvar->varno &&
			var->varattno == tlvar->varattno &&
			var->varlevelsup == tlvar->varlevelsup &&
			var->vartype == tlvar->vartype)
			return tlentry;
	}
	return NULL;
}

/*
 * hdfs_varlist_append_unique_var
 * 	Append var to var list, but only if it isn't already in the list.
 *
 * Whether a var is already a member of list is determined using varno and
 * varattno.
 */
static List *
hdfs_varlist_append_unique_var(List *varlist, Var *var)
{
	ListCell   *lc;

	foreach(lc, varlist)
	{
		Var		   *tlvar = (Var *) lfirst(lc);

		if (IsA(tlvar, Var) &&
			tlvar->varno == var->varno &&
			tlvar->varattno == var->varattno)
			return varlist;
	}

	return lappend(varlist, var);
}
#endif
