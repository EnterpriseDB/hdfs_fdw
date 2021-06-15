/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "hdfs_fdw.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST    100000.0

/* Default CPU cost to process 1 row  */
#define DEFAULT_FDW_TUPLE_COST      1000.0

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 2.0.8 so number will be 20008
 */
#define CODE_VERSION   20008

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
#if PG_VERSION_NUM >= 90500
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreigntableid,
									   ForeignPath *best_path, List *tlist,
									   List *scan_clauses, Plan *outer_plan);
#else
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreigntableid,
									   ForeignPath *best_path, List *tlist,
									   List *scan_clauses);
#endif
static void hdfsBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hdfsIterateForeignScan(ForeignScanState *node);
static void hdfsReScanForeignScan(ForeignScanState *node);
static void hdfsEndForeignScan(ForeignScanState *node);
static void hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es);
static bool hdfsAnalyzeForeignTable(Relation relation,
									AcquireSampleRowsFunc *func,
									BlockNumber *totalpages);

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

#ifdef EDB_NATIVE_LANG
#if PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 110000
#define XACT_CB_SIGNATURE XactEvent event, void *arg, bool spl_commit
#else
#define XACT_CB_SIGNATURE XactEvent event, void *arg
#endif
#else
#define XACT_CB_SIGNATURE XactEvent event, void *arg
#endif

static void hdfs_fdw_xact_callback(XACT_CB_SIGNATURE);

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
#if PG_VERSION_NUM >= 90100
							   NULL,
#endif
							   NULL,
							   NULL);

	DefineCustomStringVariable("hdfs_fdw.jvmpath",
							   "Specify the path to libjvm.so",
							   NULL,
							   &g_jvmpath,
							   "",
							   PGC_SUSET,
							   0,
#if PG_VERSION_NUM >= 90100
							   NULL,
#endif
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

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = hdfsExplainForeignScan;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = hdfsAnalyzeForeignTable;

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

	/*
	 * We use HDFSFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (HDFSFdwRelationInfo *) palloc0(sizeof(HDFSFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

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

#if PG_VERSION_NUM >= 90600
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &fpinfo->attrs_used);
#endif

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
		int		con_index;

		/* Connect to HIVE server */
		con_index = GetConnection(options, foreigntableid);

		baserel->rows = hdfs_rowcount(con_index, options, root,
									  baserel, fpinfo);

		hdfs_rel_connection(con_index);
	}
	else
		baserel->rows = 1000;

	fpinfo->rows = baserel->tuples = baserel->rows;
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
	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,	/* default pathtarget */
#endif
								   fpinfo->rows,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL, 	/* no pathkeys */
								   baserel->lateral_relids,
#if PG_VERSION_NUM >= 90500
								   NULL,	/* no extra plan */
#endif
								   NIL);	/* no fdw_private data */

	add_path(baserel, (Path *) path);
}


/*
 * hdfsGetForeignPlan
 * 		Create ForeignScan plan node which implements selected best path
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
hdfsGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
#else
static ForeignScan *
hdfsGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses)
#endif
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) baserel->fdw_private;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	hdfs_opt   *options;

	/* Get the options */
	options = hdfs_get_options(foreigntableid);

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by hdfs_classify_conditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
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
		else if (hdfs_is_foreign_expr(root, baserel, rinfo->clause))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	hdfs_deparse_select(options, &sql, root, baserel, fpinfo->attrs_used,
						&retrieved_attrs);
	if (remote_conds)
		hdfs_append_where_clause(options, &sql, root, baserel, remote_conds,
								 true, &params_list);

	/* Build the fdw_private list that will be available to the executor. */
	fdw_private = list_make2(makeString(sql.data),
							 retrieved_attrs);

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
							baserel->relid,
							params_list,
							fdw_private
#if PG_VERSION_NUM >= 90500
							,NIL
							,remote_exprs
							,outer_plan
#endif
		);
}

/*
 * hdfsBeginForeignScan
 * 		Create ForeignScan plan node which implements selected best path.
 */
static void
hdfsBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	hdfsFdwExecutionState *festate;
	Oid			foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfs_opt   *opt = hdfs_get_options(foreigntableid);
	EState	   *estate = node->ss.ps.state;

	festate = (hdfsFdwExecutionState *) palloc0(sizeof(hdfsFdwExecutionState));
	node->fdw_state = (void *) festate;

	festate->con_index = GetConnection(opt, foreigntableid);

	festate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "hdfs_fdw tuple data",
#if PG_VERSION_NUM >= 110000
											   ALLOCSET_DEFAULT_SIZES);
#else
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
#endif

	festate->query_executed = false;
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
	festate->rescan_count = 0;

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
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
	Datum	   *values;
	bool	   *nulls;
	hdfs_opt   *options;
	Oid			foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
	TupleDesc	tupdesc = node->ss.ss_currentRelation->rd_att;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MemoryContext oldcontext;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	ExecClearTuple(slot);

	/* Get the options */
	options = hdfs_get_options(foreigntableid);

	MemoryContextReset(festate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(festate->batch_cxt);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	if (!festate->query_executed)
	{
		hdfs_query_prepare(festate->con_index, options, festate->query);

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
			Oid			pgtype = TupleDescAttr(tupdesc, attnum)->atttypid;
			int32		pgtypmod = TupleDescAttr(tupdesc, attnum)->atttypmod;
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

		tuple = heap_form_tuple(tupdesc, values, nulls);

#if PG_VERSION_NUM < 120000
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);
#else
		ExecStoreHeapTuple(tuple, slot, true);
#endif
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
		Oid			foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
		hdfs_opt   *options = hdfs_get_options(foreigntableid);
		ExprContext	*econtext = node->ss.ps.ps_ExprContext;

		if (options->log_remote_sql)
			elog(LOG, "hdfs_fdw: rescan remote SQL: [%s] [%d]",
				 festate->query, festate->rescan_count++);

		hdfs_close_result_set(festate->con_index);

		if (festate->numParams > 0)
		{
			process_query_params(festate->con_index,
								 econtext,
								 festate->param_exprs,
								 festate->param_types);
		}

		festate->query_executed = hdfs_execute_prepared(festate->con_index);
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
	if (es->verbose)
	{
		List	   *fdw_private;
		char	   *sql;

		fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, 0));
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

	hdfs_analyze(con_index, options);
	totalsize = hdfs_describe(con_index, options);

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
#if PG_VERSION_NUM >= 100000
	*param_exprs = ExecInitExprList(fdw_exprs, node);
#else
	*param_exprs = (List *) ExecInitExpr((Expr *) fdw_exprs, node);
#endif
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
#if PG_VERSION_NUM >= 100000
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
#else
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);
#endif

		hdfs_bind_var(con_index, param_index + 1, param_types[param_index],
					  expr_value, &isNull);
		param_index++;
	}
}
