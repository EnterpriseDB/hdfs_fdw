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

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row  */
#define DEFAULT_FDW_TUPLE_COST		0.01

extern void _PG_init(void);

PG_FUNCTION_INFO_V1(hdfs_fdw_handler);

static void hdfs_fdw_exit(int code, Datum arg);
static hdfs_opt *GetOptions(Oid foreigntableid);

static void hdfsGetForeignRelSize(PlannerInfo *root,RelOptInfo *baserel, Oid foreigntableid);
static void hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
					Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses);
static void hdfsBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hdfsIterateForeignScan(ForeignScanState *node);
static void hdfsReScanForeignScan(ForeignScanState *node);
static void hdfsEndForeignScan(ForeignScanState *node);
static void hdfsExplainForeignScan(ForeignScanState *node, ExplainState *es);
static bool hdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
                                      BlockNumber *totalpages);

/* Initialization routine */
void
_PG_init(void)
{
	on_proc_exit(&hdfs_fdw_exit, PointerGetDatum(NULL));
}

/* Cleanup routine */
static void
hdfs_fdw_exit(int code, Datum arg)
{
	hdfs_cleanup_connection();
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

	PG_RETURN_POINTER(routine);
}

/*
 * Return the Connection and options and the connection.
 */
static hdfs_opt*
GetOptions(Oid foreigntableid)
{
	hdfs_opt      *options;
	Oid             userid =  GetUserId();
	ForeignServer   *server = NULL;
	UserMapping     *user = NULL;
	ForeignTable    *table = NULL;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = hdfs_get_options(foreigntableid);

	/* Connect to the server */
	options->conn = hdfs_get_connection(server, user, options);
	return options;
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
	ListCell              *lc = NULL;
	hdfs_opt            *options = NULL;

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
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &fpinfo->attrs_used);
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
		baserel->rows = hdfs_rowcount(options, root, baserel, fpinfo);

	fpinfo->rows = baserel->tuples = baserel->rows;
}


static void
hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) baserel->fdw_private;
	int           total_cost = 0;
	ForeignPath   *path = NULL;

	total_cost = fpinfo->fdw_tuple_cost * baserel->rows;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
								   fpinfo->rows,
								   fpinfo->fdw_startup_cost,
								   total_cost,
								   NIL,       /* no pathkeys */
								   NULL,      /* no outer rel either */
								   NIL);      /* no fdw_private list */
	add_path(baserel, (Path *) path);
}

static ForeignScan *
hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
					 ForeignPath *best_path, List *tlist, List *scan_clauses)
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
	hdfs_opt     *options = NULL;

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
							fdw_private);
}

static void
hdfsBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan              *fsplan = (ForeignScan *) node->ss.ps.plan;
	hdfsFdwExecutionState  *festate = NULL;
	Oid                      foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	GetOptions(foreigntableid);

	festate = (hdfsFdwExecutionState *) palloc(sizeof(hdfsFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->result = NULL;
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
}

static TupleTableSlot *
hdfsIterateForeignScan(ForeignScanState *node)
{
	HeapTuple      tuple;
	char           **values = NULL;
	char           *value = NULL;
	bool           *isnulls = NULL;
	unsigned int   len = 0;
	int            attid;
	hdfs_opt     *options = NULL;
	ListCell       *lc = NULL;
	AttInMetadata  *attinmeta = NULL;
	Oid                      foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	hdfsFdwExecutionState  *festate = (hdfsFdwExecutionState *) node->fdw_state;
	TupleTableSlot           *slot = node->ss.ss_ScanTupleSlot;
	TupleDesc                tupdesc = node->ss.ss_currentRelation->rd_att;

	values = palloc0(tupdesc->natts * sizeof(char*));
	isnulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	
	/* Initialize to nulls for any columns not present in result */
	memset(isnulls, true, tupdesc->natts * sizeof(bool));

	ExecClearTuple(slot);

	/* Get the options */
	options = GetOptions(foreigntableid);

	if (!festate->result)
		festate->result = hdfs_query_execute(options, festate->query);
	
	if (hdfs_fetch(options, festate->result) == HIVE_SUCCESS)
	{
		attid = 0;
		foreach(lc, festate->retrieved_attrs)
		{
			int attnum = lfirst_int(lc) - 1;
			len = hdfs_get_field_data_len(options, festate->result, attid);
			value = hdfs_get_field_as_cstring(options, festate->result, attid, &isnulls[attnum], len + 1);
			if (!isnulls[attnum])
				values[attnum] = value;
			attid++;
		}
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		tuple = BuildTupleFromCStrings(attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}
	return slot;
}

static void
hdfsReScanForeignScan(ForeignScanState *node)
{
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
	hdfs_opt        *options = NULL;
	Oid		foreigntableid = RelationGetRelid(relation);

	*func = hdfsAcquireSampleRowsFunc;
	/* Get the options */
	options = GetOptions(foreigntableid);

	hdfs_analyze(options);
	ts = hdfs_describe(options);

	*totalpages = ts/BLCKSZ;
	return true;
}

static void
hdfsEndForeignScan(ForeignScanState *node)
{
	hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
	hdfs_opt              *options = NULL;
	Oid                     foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	/* Get the options */
	options = GetOptions(foreigntableid);

	if (festate->result)
	{
		hdfs_close_result_set(options, festate->result);
		festate->result = NULL;
	}
}
