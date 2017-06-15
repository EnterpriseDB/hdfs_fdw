/*-------------------------------------------------------------------------
 *
 * hdfs_fdw.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hdfs_fdw.h"

#include "access/xact.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "storage/ipc.h"

#include <unistd.h>

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST    100.0

/* Default CPU cost to process 1 row  */
#define DEFAULT_FDW_TUPLE_COST      0.01

PG_FUNCTION_INFO_V1(hdfs_fdw_handler);

static hdfs_opt *GetOptions(Oid foreigntableid);
static int GetConnection(hdfs_opt *opt, Oid foreigntableid);

static void hdfsGetForeignRelSize(PlannerInfo *root,RelOptInfo *baserel, Oid foreigntableid);
static void hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
#if PG_VERSION_NUM >= 90500
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreigntableid, ForeignPath *best_path,
										List *tlist, List *scan_clauses, Plan *outer_plan);
#else
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreigntableid, ForeignPath *best_path,
									   List *tlist, List *scan_clauses);
#endif
static void hdfsBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hdfsIterateForeignScan(ForeignScanState *node);
static void hdfsReScanForeignScan(ForeignScanState *node);
static void hdfsEndForeignScan(ForeignScanState *node);
static void hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es);
static bool hdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
                                      BlockNumber *totalpages);

#ifdef EDB_NATIVE_LANG
	#if PG_VERSION_NUM >= 90300
		#define XACT_CB_SIGNATURE XactEvent event, void *arg, bool spl_commit
	#else
		#define XACT_CB_SIGNATURE XactEvent event, void *arg
	#endif
#else
	#define XACT_CB_SIGNATURE XactEvent event, void *arg
#endif

static void hdfs_fdw_xact_callback(XACT_CB_SIGNATURE);

void
_PG_init(void)
{
	int rc = 0;

	ereport(LOG, (errmsg("HDFS_FDW: loading ...")));

	DefineCustomStringVariable(
		"hdfs_fdw.classpath",
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

	rc = Initialize();
	if (rc == -1)
	{
		/* TODO: The error hint is linux specific */
		ereport(ERROR, (
			errmsg("could not load JVM"),
			errhint("Add path of jvm library to LD_LIBRARY_PATH")));
	}
	if (rc == -2)
	{
		ereport(ERROR, (
			errmsg("class not found"),
			errhint("Add path of HiveJdbcClient-X.X.jar to hdfs_fdw.classpath")));
	}
	if (rc < 0)
	{
		ereport(ERROR, (errmsg("Initialize failed with code %d", rc)));
	}

	ereport(LOG, (errmsg("HDFS_FDW: JVM version 0x%08X loaded", rc)));

	ereport(LOG, (errmsg("HDFS_FDW: loaded")));
}

void
_PG_fini(void)
{
	Destroy();
	ereport(LOG, (errmsg("HDFS_FDW: unloaded")));
}

/*
 * hdfs_fdw_xact_callback --- cleanup at main-transaction end.
 */
static void
hdfs_fdw_xact_callback(XACT_CB_SIGNATURE)
{
	int nestingLevel = 0;
	nestingLevel = DBCloseAllConnections();
	if (nestingLevel > 0)
	{
		if (nestingLevel > 1)
			ereport(DEBUG1, (errmsg("HDFS_FDW: %d connections closed", nestingLevel)));
		else
			ereport(DEBUG1, (errmsg("HDFS_FDW: %d connection closed", nestingLevel)));
	}
}

/*
 * Foreign-data wrapper handler function, return
 * the pointer of callback functions pointers
 */
Datum
hdfs_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize  = hdfsGetForeignRelSize;
	routine->GetForeignPlan     = hdfsGetForeignPlan;
	routine->BeginForeignScan   = hdfsBeginForeignScan;
	routine->GetForeignPaths    = hdfsGetForeignPaths;
	routine->IterateForeignScan = hdfsIterateForeignScan;
	routine->ReScanForeignScan  = hdfsReScanForeignScan;
	routine->EndForeignScan     = hdfsEndForeignScan;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = hdfsExplainForeignScan;
	
	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = hdfsAnalyzeForeignTable;

	RegisterXactCallback(hdfs_fdw_xact_callback, NULL);

	PG_RETURN_POINTER(routine);
}

int
GetConnection(hdfs_opt* opt, Oid foreigntableid)
{
	Oid             userid =  GetUserId();
	ForeignServer   *server = NULL;
	ForeignTable    *table = NULL;
	UserMapping     *user = NULL;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Connect to the server */
	return hdfs_get_connection(server,user, opt);
}


/*
 * Return the Connection and options and the connection.
 */
static hdfs_opt*
GetOptions(Oid foreigntableid)
{
	/* Fetch the options */
	return hdfs_get_options(foreigntableid);
}


/*
 * hdfsGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
hdfsGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	HDFSFdwRelationInfo *fpinfo = NULL;
	ListCell            *lc = NULL;
	hdfs_opt            *options = NULL;
	int					con_index = -1;

	/*
	 * We use HDFSFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (HDFSFdwRelationInfo *) palloc0(sizeof(HDFSFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Get the options */
	options = GetOptions(foreigntableid);

	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
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
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
#endif
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	baserel->rows = 1000;

	/* Get the actual number of rows from server
	 * if use_remote_estimate is specified in options.
	 */
	if (options->use_remote_estimate)
	{
		/* Connect to HIVE server */
		con_index = GetConnection(options, foreigntableid);

		baserel->rows = hdfs_rowcount(con_index, options, root,
									baserel, fpinfo);

		hdfs_rel_connection(con_index);
	}

	fpinfo->rows = baserel->tuples = baserel->rows;
}

static void
hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) baserel->fdw_private;
	int                 total_cost = 0;
	ForeignPath         *path = NULL;

	total_cost = fpinfo->fdw_tuple_cost * baserel->rows;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,  /* default pathtarget */
#endif
								   fpinfo->rows,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL,       /* no pathkeys */
								   NULL,      /* no outer rel either */
#if PG_VERSION_NUM >= 90500
								   NULL, /* no extra plan */
#endif
								   NIL); /* no fdw_private data */
	add_path(baserel, (Path *) path);
}


/*
 * hdfsGetForeignPlan
 *              Create ForeignScan plan node which implements selected best path
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
static ForeignScan*
hdfsGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses)
#endif
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) baserel->fdw_private;
	Index          scan_relid = baserel->relid;
	List           *fdw_private;
	List           *remote_conds = NIL;
	List           *local_exprs = NIL;
	List           *params_list = NIL;
	List           *retrieved_attrs;
	StringInfoData sql;
	ListCell       *lc = NULL;
	hdfs_opt       *options = NULL;

	/* Get the options */
	options = GetOptions(foreigntableid);

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by classifyConditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * Note: the join clauses we see here should be the exact same ones
	 * previously examined by postgresGetForeignPaths.  Possibly it'd be worth
	 * passing forward the classification work done then, rather than
	 * repeating it here.
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
			remote_conds = lappend(remote_conds, rinfo);
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (is_foreign_expr(root, baserel, rinfo->clause))
			remote_conds = lappend(remote_conds, rinfo);
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

	elog(DEBUG1, "Remote SQL: %s", sql.data);
	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
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
							scan_relid,
							params_list,
							fdw_private
#if PG_VERSION_NUM >= 90500
							,NIL
							,NIL
							,NULL
#endif
							);
}

static void
hdfsBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan              *fsplan = (ForeignScan *) node->ss.ps.plan;
	hdfsFdwExecutionState    *festate = NULL;
	Oid                      foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfs_opt                 *opt = GetOptions(foreigntableid);
	EState                   *estate = node->ss.ps.state;

	festate = (hdfsFdwExecutionState *) palloc(sizeof(hdfsFdwExecutionState));

	/* Connect to HIVE server */
	festate->con_index = GetConnection(opt, foreigntableid);

	node->fdw_state = (void *) festate;
	festate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
										   "hdfs_fdw tuple data",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	festate->col_list = hdfs_desc_query(festate->con_index, opt);
	festate->query_executed = false;
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
	return;
}

static TupleTableSlot *
hdfsIterateForeignScan(ForeignScanState *node)
{
	HeapTuple              tuple;
	Datum	               *values;
	bool	               *nulls;
	char                   *value = NULL;
	int                    attid;
	hdfs_opt               *options = NULL;
	ListCell               *lc = NULL;
	Oid                    foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfsFdwExecutionState  *festate = (hdfsFdwExecutionState *) node->fdw_state;
	TupleDesc              tupdesc = node->ss.ss_currentRelation->rd_att;
	TupleTableSlot         *slot = node->ss.ss_ScanTupleSlot;
	MemoryContext			oldcontext = NULL;
	int						r;

	ExecClearTuple(slot);
	/* Get the options */

	options = GetOptions(foreigntableid);

	MemoryContextReset(festate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(festate->batch_cxt);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	if (!festate->query_executed)
	{
		festate->query_executed = hdfs_query_execute(festate->con_index,
													options,
													festate->query);
	}

	r = hdfs_fetch(festate->con_index, options);
	if (r == 0)
	{
		attid = 0;
		foreach(lc, festate->retrieved_attrs)
		{
			int         len;
			bool        isnull = true;
			int         attnum = lfirst_int(lc) - 1;
			Oid         pgtype = tupdesc->attrs[attnum]->atttypid;
			int32       pgtypmod = tupdesc->attrs[attnum]->atttypmod;
			Datum       v;
			char        *attrname;
			hdfs_column *cols = NULL;
			ListCell    *list_cell;

			attrname = NameStr(tupdesc->attrs[attnum]->attname);
			foreach(list_cell, festate->col_list)
			{
				cols = (hdfs_column *) lfirst(list_cell);

				if (cols != NULL && strcmp(cols->col_name, attrname) == 0)
					break;
			}
			if (cols)
			{
				v = hdfs_get_value(festate->con_index, options, pgtype,
									pgtypmod, attid, &isnull,
									cols->col_type);
				if (!isnull)
				{
					nulls[attnum] = false;
					values[attnum] = v;
				}
				attid++;
			}
		}
		tuple = heap_form_tuple(tupdesc, values, nulls);
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);
	}
	MemoryContextSwitchTo(oldcontext);
	return slot;
}

static void
hdfsReScanForeignScan(ForeignScanState *node)
{
	Oid                    foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfsFdwExecutionState  *festate = (hdfsFdwExecutionState *) node->fdw_state;
	hdfs_opt               *options = NULL;

	/* Get the options */
	options = GetOptions(foreigntableid);

	if (festate->query_executed)
	{
		hdfs_close_result_set(festate->con_index, options);
		festate->query_executed = hdfs_query_execute(festate->con_index,
													options,
													festate->query);
	}
	return;
}

static void
hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List     *fdw_private = NULL;
	char     *sql = NULL;

	if (es->verbose)
	{
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
static bool
hdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	long		ts = 0;
	hdfs_opt	*options = NULL;
	Oid			foreigntableid = RelationGetRelid(relation);
	int			con_index = -1;

	*func = hdfsAcquireSampleRowsFunc;

	/* Get the options */
	options = GetOptions(foreigntableid);

	/* Connect to HIVE server */
	con_index = GetConnection(options, foreigntableid);

	hdfs_analyze(con_index, options);
	ts = hdfs_describe(con_index, options);

	*totalpages = ts/BLCKSZ;
	return true;
}

static void
hdfsEndForeignScan(ForeignScanState *node)
{
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
	hdfs_opt              *options = NULL;
	Oid                   foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	/* Get the options */
	options = GetOptions(foreigntableid);

	if (festate->query_executed)
	{
		hdfs_close_result_set(festate->con_index, options);
		festate->query_executed = false;
	}
	hdfs_rel_connection(festate->con_index);
	return;
}
