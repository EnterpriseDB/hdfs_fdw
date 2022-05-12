/*-------------------------------------------------------------------------
 *
 * hdfs_deparse.c
 * 		Query deparser for hdfs_fdw.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_deparse.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "hdfs_fdw.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Global context for hdfs_foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */

	/*
	 * For join pushdown, only a limited set of operators are allowed to be
	 * pushed.  This flag helps us identify if we are walking through the list
	 * of join conditions.  Also true for aggregate relations to restrict
	 * aggregates for specified list.
	 */
	bool		is_remote_cond;	/* true for join or aggregate relations */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for hdfs_foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for hdfs_deparse_expr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
	appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool hdfs_foreign_expr_walker(Node *node,
									 foreign_glob_cxt *glob_cxt,
									 foreign_loc_cxt *outer_cxt);
static bool hdfs_is_builtin(Oid procid);

/*
 * Functions to construct string representation of a node tree.
 */
static void hdfs_deparse_target_list(StringInfo buf,
									 PlannerInfo *root,
									 Index rtindex,
									 Relation rel,
									 Bitmapset *attrs_used,
									 List **retrieved_attrs);
static char *hdfs_quote_identifier(const char *str, char quotechar);
static void hdfs_deparse_column_ref(StringInfo buf, int varno, int varattno,
									PlannerInfo *root, bool qualify_col);
static void hdfs_deparse_relation(StringInfo buf, Relation rel);
static void hdfs_deparse_expr(Expr *expr, deparse_expr_cxt *context);
static void hdfs_deparse_var(Var *node, deparse_expr_cxt *context);
static void hdfs_deparse_const(Const *node, deparse_expr_cxt *context);
static void hdfs_deparse_param(Param *node, deparse_expr_cxt *context);
#if PG_VERSION_NUM < 120000
static void hdfs_deparse_array_ref(ArrayRef *node, deparse_expr_cxt *context);
#else
static void hdfs_deparse_subscripting_ref(SubscriptingRef *node,
										  deparse_expr_cxt *context);
#endif
static void hdfs_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void hdfs_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void hdfs_deparse_operator_name(StringInfo buf,
									   Form_pg_operator opform);
static void hdfs_deparse_distinct_expr(DistinctExpr *node,
									   deparse_expr_cxt *context);
static void hdfs_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
											  deparse_expr_cxt *context);
static void hdfs_deparse_relabel_type(RelabelType *node,
									  deparse_expr_cxt *context);
static void hdfs_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void hdfs_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void hdfs_deparse_array_expr(ArrayExpr *node,
									deparse_expr_cxt *context);
static void hdfs_deparse_string_literal(StringInfo buf, const char *val);
static void hdfs_print_remote_param(deparse_expr_cxt *context);
static void hdfs_print_remote_placeholder(deparse_expr_cxt *context);
static void hdfs_append_conditions(List *exprs, deparse_expr_cxt *context);
static void hdfs_deparse_select_sql(List *tlist, bool is_subquery,
									List **retrieved_attrs,
									deparse_expr_cxt *context);
static void hdfs_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
										   RelOptInfo *foreignrel,
										   bool use_alias, List **params_list);
static void hdfs_deparse_from_expr(List *quals, deparse_expr_cxt *context,
								   bool use_alias);
static void hdfs_deparse_rangeTblRef(StringInfo buf, PlannerInfo *root,
									 RelOptInfo *foreignrel, bool make_subquery,
									 List **params_list);
static void hdfs_deparse_explicit_target_list(List *tlist,
											  List **retrieved_attrs,
											  deparse_expr_cxt *context);
static void hdfs_deparse_subquery_target_list(deparse_expr_cxt *context);
static void hdfs_append_function_name(Oid funcid, deparse_expr_cxt *context);
static void hdfs_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void hdfs_append_groupby_clause(List *tlist, deparse_expr_cxt *context);
static Node *hdfs_deparse_sort_group_clause(Index ref, List *tlist,
											deparse_expr_cxt *context);

/*
 * Helper functions
 */
static bool hdfs_is_subquery_var(Var *node, RelOptInfo *foreignrel,
								 int *relno, int *colno,
								 deparse_expr_cxt *context);
static void hdfs_get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
											   int *relno, int *colno,
											   deparse_expr_cxt *context);

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *  - remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
hdfs_classify_conditions(PlannerInfo *root,
						 RelOptInfo *baserel,
						 List *input_conds,
						 List **remote_conds,
						 List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (hdfs_is_foreign_expr(root, baserel, ri->clause, false))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
hdfs_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr,
					 bool is_remote_cond)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.is_remote_cond = is_remote_cond;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!hdfs_foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/* Expressions examined here should be boolean, i.e. noncollatable */
	Assert(loc_cxt.collation == InvalidOid);
	Assert(loc_cxt.state == FDW_COLLATE_NONE);

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is
 * pretty close to assign_collations_walker() in parse_collate.c, though we
 * can assume here that the given expression is valid.
 */
static bool
hdfs_foreign_expr_walker(Node *node,
						 foreign_glob_cxt *glob_cxt,
						 foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */
					if (var->varattno < 0 &&
						var->varattno != SelfItemPointerAttributeNumber)
						return false;
				}
			}
			break;
		case T_Const:
		case T_Param:
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			{
				ArrayRef   *ar = (ArrayRef *) node;;

				/* Assignment should not be in restrictions. */
				if (ar->refassgnexpr != NULL)
					return false;

				/*
				 * Recurse to remaining subexpressions.  Since the array
				 * subscripts must yield (noncollatable) integers, they won't
				 * affect the inner_cxt state.
				 */
				if (!hdfs_foreign_expr_walker((Node *) ar->refupperindexpr,
											  glob_cxt, &inner_cxt))
					return false;
				if (!hdfs_foreign_expr_walker((Node *) ar->reflowerindexpr,
											  glob_cxt, &inner_cxt))
					return false;
				if (!hdfs_foreign_expr_walker((Node *) ar->refexpr,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
#else
		case T_SubscriptingRef:
			{
				SubscriptingRef *sbref = (SubscriptingRef *) node;;

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/* Assignment should not be in restrictions. */
				if (sbref->refassgnexpr != NULL)
					return false;

				/*
				 * Recurse to remaining subexpressions.  Since the array
				 * subscripts must yield (noncollatable) integers, they won't
				 * affect the inner_cxt state.
				 */
				if (!hdfs_foreign_expr_walker((Node *) sbref->refupperindexpr,
											  glob_cxt, &inner_cxt))
					return false;
				if (!hdfs_foreign_expr_walker((Node *) sbref->reflowerindexpr,
											  glob_cxt, &inner_cxt))
					return false;
				if (!hdfs_foreign_expr_walker((Node *) sbref->refexpr,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
#endif
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/*
				 * If function used by the expression is not built-in, it
				 * can't be sent to remote because it might have incompatible
				 * semantics on remote side.
				 */
				if (!hdfs_is_builtin(fe->funcid))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) fe->args,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			{
				OpExpr	   *oe = (OpExpr *) node;
				const char *operatorName = get_opname(oe->opno);

				/*
				 * Join-pushdown allows only a few operators to be pushed down.
				 */
				if (glob_cxt->is_remote_cond &&
					(!(strcmp(operatorName, "<") == 0 ||
					   strcmp(operatorName, ">") == 0 ||
					   strcmp(operatorName, "<=") == 0 ||
					   strcmp(operatorName, ">=") == 0 ||
					   strcmp(operatorName, "<>") == 0 ||
					   strcmp(operatorName, "=") == 0 ||
					   strcmp(operatorName, "+") == 0 ||
					   strcmp(operatorName, "-") == 0 ||
					   strcmp(operatorName, "*") == 0 ||
					   strcmp(operatorName, "%") == 0 ||
					   strcmp(operatorName, "/") == 0)))
					return false;

				/*
				 * Similarly, only built-in operators can be sent to remote.
				 * (If the operator is, surely its underlying function is
				 * too.)
				 */
				if (!hdfs_is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) oe->args,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!hdfs_is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) oe->args,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!hdfs_foreign_expr_walker((Node *) r->arg,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) b->args,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) nt->arg,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!hdfs_foreign_expr_walker((Node *) a->elements,
											  glob_cxt, &inner_cxt))
					return false;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!hdfs_foreign_expr_walker((Node *) lfirst(lc),
												  glob_cxt, &inner_cxt))
						return false;
				}

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;
				const char *func_name;

				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/* Aggregates with order are not supported on hive/spark. */
				if (agg->aggorder)
					return false;

				/* FILTER clause is not supported on hive/spark. */
				if (agg->aggfilter)
					return false;

				/* VARIADIC not supported on hive/spark. */
				if (agg->aggvariadic)
					return false;

				/* As usual, it must be shippable. */
				if (!hdfs_is_builtin(agg->aggfnoid))
					return false;

				func_name = get_func_name(agg->aggfnoid);
				if (!(strcmp(func_name, "min") == 0 ||
					strcmp(func_name, "max") == 0 ||
					strcmp(func_name, "sum") == 0 ||
					strcmp(func_name, "avg") == 0 ||
					strcmp(func_name, "count") == 0))
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!hdfs_foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !hdfs_is_builtin(exprType(node)))
		return false;

	/* It looks OK */
	return true;
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
static bool
hdfs_is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
}

void
hdfs_deparse_explain(hdfs_opt *opt, StringInfo buf)
{
	appendStringInfo(buf, "EXPLAIN SELECT * FROM ");
	appendStringInfo(buf, "%s.%s", hdfs_quote_identifier(opt->dbname, '`'),
					 hdfs_quote_identifier(opt->table_name, '`'));

	/*
	 * For accurate row counts we should append where clauses with the
	 * statement, but if where clause is parameterized we should handle it the
	 * way postgres fdw does.
	 * TODO:
	 * if (fpinfo->remote_conds)
	 * 	hdfs_append_where_clause(opt, buf, root, baserel, fpinfo->remote_conds,
	 * true, &params_list);
	 */
}

void
hdfs_deparse_describe(StringInfo buf, Relation rel)
{
	appendStringInfo(buf, "DESCRIBE FORMATTED ");
	hdfs_deparse_relation(buf, rel);
}

void
hdfs_deparse_analyze(StringInfo buf, Relation rel)
{
	appendStringInfo(buf, "ANALYZE TABLE ");
	hdfs_deparse_relation(buf, rel);
	appendStringInfo(buf, " COMPUTE STATISTICS");
}

/*
 * hdfs_deparse_select_stmt_for_rel
 * 		Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign
 * server.  For a base relation fpinfo->attrs_used is used to construct
 * SELECT clause, hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause.
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
hdfs_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root,
								 RelOptInfo *rel, List *tlist,
								 List *remote_conds, bool is_subquery,
								 List **retrieved_attrs,
								 List **params_list)
{
	deparse_expr_cxt context;
	List	   *quals;
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) rel->fdw_private;

	/*
	 * We handle relations for foreign tables and joins between those and upper
	 * relations
	 */
	Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

	/* Fill portions of context common to base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.params_list = params_list;
	context.scanrel = IS_UPPER_REL(rel) ? fpinfo->outerrel : rel;

	/* Construct SELECT clause */
	hdfs_deparse_select_sql(tlist, is_subquery, retrieved_attrs, &context);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(rel))
	{
		HDFSFdwRelationInfo *ofpinfo;

		ofpinfo = (HDFSFdwRelationInfo *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	hdfs_deparse_from_expr(quals, &context, is_subquery);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		hdfs_append_groupby_clause(fpinfo->grouped_tlist, &context);

		/* Append HAVING clause */
		if (remote_conds)
		{
			appendStringInfoString(buf, " HAVING ");
			hdfs_append_conditions(remote_conds, &context);
		}
	}
}

/*
 * hdfs_deparse_select_sql
 * 			Construct a simple SELECT statement that retrieves desired columns
 * 			of the specified foreign table, and append it to "buf".  The output
 * 			contains just "SELECT ...".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns. is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void
hdfs_deparse_select_sql(List *tlist, bool is_subquery, List **retrieved_attrs,
						deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	PlannerInfo *root = context->root;

	appendStringInfoString(buf, "SELECT ");

	if (is_subquery)
	{
		/*
		 * For a relation that is deparsed as a subquery, emit expressions
		 * specified in the relation's reltarget.  Note that since this is for
		 * the subquery, no need to care about *retrieved_attrs.
		 */
		hdfs_deparse_subquery_target_list(context);

	}
	else if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		/*
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		hdfs_deparse_explicit_target_list(tlist, retrieved_attrs, context);
	}
	else
	{
		HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
		Relation	rel;

		/*
		 * Core code already has some lock on each rel being planned, so we can
		 * use NoLock here.
		 */
#if PG_VERSION_NUM < 130000
		rel = heap_open(rte->relid, NoLock);
#else
		rel = table_open(rte->relid, NoLock);
#endif

		/* Construct target list */
		hdfs_deparse_target_list(buf, root, foreignrel->relid, rel,
								 fpinfo->attrs_used, retrieved_attrs);

#if PG_VERSION_NUM < 130000
		heap_close(rel, NoLock);
#else
		table_close(rel, NoLock);
#endif
	}
}

/*
 * hdfs_deparse_from_expr
 * 		Construct a FROM clause and, if needed, a WHERE clause, and
 * 		append those to "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 */
static void
hdfs_deparse_from_expr(List *quals, deparse_expr_cxt *context, bool use_alias)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;

	Assert(!IS_UPPER_REL(context->foreignrel) ||
		   IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

	use_alias |= (bms_membership(scanrel->relids) == BMS_MULTIPLE);

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	hdfs_deparse_from_expr_for_rel(buf, context->root, scanrel,
								   use_alias, context->params_list);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
		hdfs_append_conditions(quals, context);
	}
}

/*
 * hdfs_deparse_explicit_target_list
 * 		Deparse given targetlist and append it to context->buf.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 */
static void
hdfs_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
								  deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");

		hdfs_deparse_expr((Expr *) lfirst(lc), context);
		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0)
		appendStringInfoString(buf, "NULL");
}

/*
 * hdfs_deparse_subquery_target_list
 *
 * Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void
hdfs_deparse_subquery_target_list(deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	bool		first;
	ListCell   *lc;
	int			i;
	List	   *scan_var_list = NIL;
	List	   *whole_row_lists = NIL;


	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
									PVC_RECURSE_PLACEHOLDERS);
	scan_var_list = hdfs_adjust_whole_row_ref(context->root, scan_var_list,
											  &whole_row_lists,
											  foreignrel->relids);

	first = true;
	i = 1;
	foreach(lc, scan_var_list)
	{
		Node	   *node = (Node *) lfirst(lc);

		if (!first)
			appendStringInfo(buf, " %s%d, ", SUBQUERY_COL_ALIAS_PREFIX, i++);
		first = false;

		hdfs_deparse_expr((Expr *) node, context);
	}

	/* Don't generate bad syntax if no expressions */
	if (first)
		appendStringInfoString(buf, "NULL");
	else
	{
		/* Append the column alias for the last expression */
		appendStringInfo(buf, " %s%d", SUBQUERY_COL_ALIAS_PREFIX, i++);
	}
}

/*
 * hdfs_append_conditions
 * 		Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed.  This function is used
 * to deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void
hdfs_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/*
		 * Extract clause from RestrictInfo, if required. See comments in
		 * declaration of HDFSFdwRelationInfo for details.
		 */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;

			expr = ri->clause;
		}

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		hdfs_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 *
 * The target list text is appended to buf, and we also create an integer
 * list of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
hdfs_deparse_target_list(StringInfo buf,
						 PlannerInfo *root,
						 Index rtindex,
						 Relation rel,
						 Bitmapset *attrs_used,
						 List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;
	bool		first = true;
	bool		have_wholerow = false;

	*retrieved_attrs = NIL;

	/*
	 * If whole-row reference is used or all the columns in the table are
	 * referenced, instead of sending all the column's list, send 'SELECT *'
	 * query to avoid the Map-reduce job.
	 */
	if (attrs_used != NULL &&
		(bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used) ||
		 tupdesc->natts == bms_num_members(attrs_used)))
	{
		have_wholerow = true;
		appendStringInfoChar(buf, '*');
	}

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			if (!have_wholerow)
			{
				if (!first)
					appendStringInfoString(buf, ", ");

				first = false;
				hdfs_deparse_column_ref(buf, rtindex, i, root, false);
			}

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first && !have_wholerow)
		appendStringInfoString(buf, "NULL");
}

static char *
hdfs_quote_identifier(const char *str, char quotechar)
{
	char       *result = palloc(strlen(str) * 2 + 3);
	char       *res = result;

	*res++ = quotechar;
	while (*str)
	{
		if (*str == quotechar)
			*res++ = *str;
		*res++ = *str;
		str++;
	}
	*res++ = quotechar;
	*res = '\0';

	return result;
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
hdfs_deparse_column_ref(StringInfo buf, int varno, int varattno,
						PlannerInfo *root, bool qualify_col)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
#if PG_VERSION_NUM >= 110000
		colname = get_attname(rte->relid, varattno, false);
#else
		colname = get_relid_attribute_name(rte->relid, varattno);
#endif

	if (qualify_col)
		ADD_REL_QUALIFIER(buf, varno);

	appendStringInfoString(buf, hdfs_quote_identifier(colname, '`'));
}

/*
 * hdfs_deparse_from_expr_for_rel
 * 		Construct a FROM clause and, if needed, a WHERE clause, and append
 * 		those to "buf".
 *
 *  quals is the list of clauses to be included in the WHERE clause.
 */
static void
hdfs_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
							   RelOptInfo *foreignrel, bool use_alias,
							   List **params_list)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;

	if (IS_JOIN_REL(foreignrel))
	{
		RelOptInfo *rel_o = fpinfo->outerrel;
		RelOptInfo *rel_i = fpinfo->innerrel;
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;

		/* Deparse outer relation */
		initStringInfo(&join_sql_o);
		hdfs_deparse_rangeTblRef(&join_sql_o, root, rel_o,
								 fpinfo->make_outerrel_subquery, params_list);

		/* Deparse inner relation */
		initStringInfo(&join_sql_i);
		hdfs_deparse_rangeTblRef(&join_sql_i, root, rel_i,
								 fpinfo->make_innerrel_subquery, params_list);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * ((outer relation) <join type> (inner relation) ON (joinclauses)
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ", join_sql_o.data,
						 hdfs_get_jointype_name(fpinfo->jointype),
						 join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.scanrel = foreignrel;
			context.root = root;
			context.params_list = params_list;

			appendStringInfo(buf, "(");
			hdfs_append_conditions(fpinfo->joinclauses, &context);
			appendStringInfo(buf, ")");
		}
		else
			appendStringInfoString(buf, "(TRUE)");

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
#if PG_VERSION_NUM < 130000
		Relation	rel = heap_open(rte->relid, NoLock);
#else
		Relation	rel = table_open(rte->relid, NoLock);
#endif

		hdfs_deparse_relation(buf, rel);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX,
							 foreignrel->relid);

#if PG_VERSION_NUM < 130000
		heap_close(rel, NoLock);
#else
		table_close(rel, NoLock);
#endif
	}
	return;
}

/*
 * hdfs_deparse_rangeTblRef
 *
 * Append FROM clause entry for the given relation into buf.
 */
static void
hdfs_deparse_rangeTblRef(StringInfo buf, PlannerInfo *root,
						 RelOptInfo *foreignrel, bool make_subquery,
						 List **params_list)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	Assert(fpinfo->local_conds == NIL);

	/* If make_subquery is true, deparse the relation as a subquery. */
	if (make_subquery)
	{
		List	   *retrieved_attrs;
		int			ncols;

		/* Deparse the subquery representing the relation. */
		appendStringInfoChar(buf, '(');
		hdfs_deparse_select_stmt_for_rel(buf, root, foreignrel, NIL,
										 fpinfo->remote_conds, true,
										 &retrieved_attrs, params_list);
		appendStringInfoChar(buf, ')');

		/* Append the relation alias. */
		appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX,
						 fpinfo->relation_index);
	}
	else
		hdfs_deparse_from_expr_for_rel(buf, root, foreignrel, true,
									   params_list);
}

/*
 * hdfs_get_jointype_name
 * 		Output join name for given join type
 */
const char *
hdfs_get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * hdfs_deparse_relation
 *			Append remote name of specified foreign table to buf.
 *
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, dbname FDW option overrides database name.
 */
static void
hdfs_deparse_relation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *dbname = NULL;
	const char *relname = NULL;
	ListCell   *lc;

	/* Obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "dbname") == 0)
			dbname = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	if (dbname == NULL)
		dbname = DEFAULT_DATABASE;
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s.%s", hdfs_quote_identifier(dbname, '`'),
					 hdfs_quote_identifier(relname, '`'));
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
static void
hdfs_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);

	appendStringInfoChar(buf, '\'');

	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);

		appendStringInfoChar(buf, ch);
	}

	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that
 * hdfs_foreign_expr_walker accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
hdfs_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			hdfs_deparse_var((Var *) node, context);
			break;
		case T_Const:
			hdfs_deparse_const((Const *) node, context);
			break;
		case T_Param:
			hdfs_deparse_param((Param *) node, context);
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			hdfs_deparse_array_ref((ArrayRef *) node, context);
#else
		case T_SubscriptingRef:
			hdfs_deparse_subscripting_ref((SubscriptingRef *) node, context);
			break;
#endif
		case T_FuncExpr:
			hdfs_deparse_func_expr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			hdfs_deparse_op_expr((OpExpr *) node, context);
			break;
		case T_DistinctExpr:
			hdfs_deparse_distinct_expr((DistinctExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			hdfs_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node,
											  context);
			break;
		case T_RelabelType:
			hdfs_deparse_relabel_type((RelabelType *) node, context);
			break;
		case T_BoolExpr:
			hdfs_deparse_bool_expr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			hdfs_deparse_null_test((NullTest *) node, context);
			break;
		case T_ArrayExpr:
			hdfs_deparse_array_expr((ArrayExpr *) node, context);
			break;
		case T_Aggref:
			hdfs_deparse_aggref((Aggref *) node, context);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported expression type for deparse: %d",
							(int) nodeTag(node))));
			break;
	}
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * hdfs_deparse_param for comments.
 */
static void
hdfs_deparse_var(Var *node, deparse_expr_cxt *context)
{
	Relids		relids = context->scanrel->relids;
	bool		qualify_col;
	int			relno;
	int			colno;

	qualify_col = (bms_membership(relids) == BMS_MULTIPLE);

	/*
	 * If the Var belongs to the foreign relation that is deparsed as a
	 * subquery, use the relation and column alias to the Var provided
	 * by the subquery, instead of the remote name.
	 */
	if (hdfs_is_subquery_var(node, context->foreignrel, &relno, &colno, context))
	{
		appendStringInfo(context->buf, "%s%d.%s%d",
						 SUBQUERY_REL_ALIAS_PREFIX, relno,
						 SUBQUERY_COL_ALIAS_PREFIX, colno);
		return;
	}

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		hdfs_deparse_column_ref(context->buf, node->varno, node->varattno,
								context->root, qualify_col);
	}
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = list_length(*context->params_list) + 1;

			*context->params_list = lappend(*context->params_list, node);
			hdfs_print_remote_param(context);
		}
		else
			hdfs_print_remote_placeholder(context);
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 */
static void
hdfs_deparse_const(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	switch (node->consttype)
	{
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case INTERVALOID:
		case TIMETZOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			appendStringInfo(buf, "'%s'", extval);
			break;
		case BITOID:
		case VARBITOID:
			appendStringInfo(buf, "B'%s'", extval);
			break;
		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;
		default:
			hdfs_deparse_string_literal(buf, extval);
			break;
	}
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list, and then use its index in that list as the remote
 * parameter number. During EXPLAIN, there's no need to identify a parameter
 * number.
 */
static void
hdfs_deparse_param(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int			pindex = list_length(*context->params_list) + 1;

		*context->params_list = lappend(*context->params_list, node);
		hdfs_print_remote_param(context);
	}
	else
		hdfs_print_remote_placeholder(context);
}

/*
 * Deparse an array subscript expression.
 */
static void
#if PG_VERSION_NUM < 120000
hdfs_deparse_array_ref(ArrayRef *node, deparse_expr_cxt *context)
#else
hdfs_deparse_subscripting_ref(SubscriptingRef *node,
							  deparse_expr_cxt *context)
#endif
{
	StringInfo	buf = context->buf;
	ListCell   *lowlist_item;
	ListCell   *uplist_item;

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/*
	 * Deparse referenced array expression first.  If that expression includes
	 * a cast, we have to parenthesize to prevent the array subscript from
	 * being taken as typename decoration.  We can avoid that in the typical
	 * case of subscripting a Var, but otherwise do it.
	 */
	if (IsA(node->refexpr, Var))
		hdfs_deparse_expr(node->refexpr, context);
	else
	{
		appendStringInfoChar(buf, '(');
		hdfs_deparse_expr(node->refexpr, context);
		appendStringInfoChar(buf, ')');
	}

	/* Deparse subscript expressions. */
	lowlist_item = list_head(node->reflowerindexpr);	/* could be NULL */
	foreach(uplist_item, node->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');

		if (lowlist_item)
		{
			hdfs_deparse_expr(lfirst(lowlist_item), context);
			appendStringInfoChar(buf, ':');
#if PG_VERSION_NUM < 130000
			lowlist_item = lnext(lowlist_item);
#else
			lowlist_item = lnext(node->reflowerindexpr, lowlist_item);
#endif
		}

		hdfs_deparse_expr(lfirst(uplist_item), context);
		appendStringInfoChar(buf, ']');
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a function call.
 */
static void
hdfs_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	bool		first;
	ListCell   *arg;

	/* Normal function: display as proname(args) */
	hdfs_append_function_name(node->funcid, context);

	appendStringInfoChar(buf, '(');

	/* Deparse all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");

		hdfs_deparse_expr((Expr *) lfirst(arg), context);
		first = false;
	}

	appendStringInfoChar(buf, ')');
}

/*
 * hdfs_append_function_name
 *		Deparses function name from given function oid.
 */
static void
hdfs_append_function_name(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Deparse the function name ... */
	proname = NameStr(procform->proname);
	appendStringInfoString(buf, proname);

	ReleaseSysCache(proctup);
}

/*
 * Deparse given operator expression. To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
hdfs_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		hdfs_deparse_expr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	hdfs_deparse_operator_name(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		hdfs_deparse_expr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
hdfs_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	char	   *opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);

		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 quote_identifier(opnspname), opname);
	}
	else
	{
		if (strcmp(opname, "~~") == 0)
			appendStringInfoString(buf, "LIKE");
		else if (strcmp(opname, "~~*") == 0)
			appendStringInfoString(buf, "LIKE");
		else if (strcmp(opname, "!~~") == 0)
			appendStringInfoString(buf, "NOT LIKE");
		else if (strcmp(opname, "!~~*") == 0)
			appendStringInfoString(buf, "NOT LIKE");
		else if (strcmp(opname, "~") == 0)
		{
			/* TODO: use hive operator */
		}
		else if (strcmp(opname, "~*") == 0)
		{
			/* TODO: use hive operator */
		}
		else if (strcmp(opname, "!~") == 0)
		{
			/* TODO: use hive operator */
		}
		else if (strcmp(opname, "!~*") == 0)
		{
			/* TODO: use hive operator */
		}
		else
		{
			/* Just print operator name. */
			appendStringInfoString(buf, opname);
		}
	}
}

/*
 * Deparse IS DISTINCT FROM.
 */
static void
hdfs_deparse_distinct_expr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	hdfs_deparse_expr(linitial(node->args), context);
	appendStringInfoString(buf, " IS DISTINCT FROM ");
	hdfs_deparse_expr(lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

static void
hdfs_deparse_string(StringInfo buf, const char *val, bool isstr)
{
	const char *valptr;
	int			i;

	if (isstr)
		appendStringInfoChar(buf, '\'');

	for (valptr = val, i = 0; *valptr; valptr++, i++)
	{
		char		ch = *valptr;

		/*
		 * Remove '{', '}' and \" character from the string. Because this
		 * syntax is not recognize by the remote HiveServer.
		 */
		if ((ch == '{' && i == 0) ||
			(ch == '}' && (i == (strlen(val) - 1))) || ch == '\"')
			continue;

		if (ch == ',' && isstr)
			appendStringInfoString(buf, "', '");
		else
			appendStringInfoChar(buf, ch);
	}

	if (isstr)
		appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
hdfs_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
								  deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Expr	   *arg1;
	Expr	   *arg2;
	Form_pg_operator form;
	char	   *opname;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Deparse left operand. */
	arg1 = linitial(node->args);
	hdfs_deparse_expr(arg1, context);
	appendStringInfoChar(buf, ' ');

	opname = NameStr(form->oprname);
	if (strcmp(opname, "<>") == 0)
		appendStringInfoString(buf, " NOT ");

	/* Deparse operator name plus decoration. */
	appendStringInfoString(buf, " IN ");
	appendStringInfoChar(buf, '(');

	/* Deparse right operand. */
	arg2 = lsecond(node->args);
	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				Const	   *c = (Const *) arg2;

				if (c->constisnull)
				{
					appendStringInfoString(buf, " NULL");
					ReleaseSysCache(tuple);
					return;
				}

				getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, c->constvalue);

				switch (c->consttype)
				{
					case INT4ARRAYOID:
					case OIDARRAYOID:
						hdfs_deparse_string(buf, extval, false);
						break;
					default:
						hdfs_deparse_string(buf, extval, true);
						break;
				}
			}
			break;
		default:
			hdfs_deparse_expr(arg2, context);
			break;
	}

	appendStringInfoChar(buf, ')');
	ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
hdfs_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	hdfs_deparse_expr(node->arg, context);
}

/*
 * Deparse a BoolExpr node.
 */
static void
hdfs_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoChar(buf, '(');
			appendStringInfoString(buf, "NOT ");
			hdfs_deparse_expr(linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		hdfs_deparse_expr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
hdfs_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	hdfs_deparse_expr(node->arg, context);

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL");
	else
		appendStringInfoString(buf, " IS NOT NULL");

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
hdfs_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "ARRAY[");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		hdfs_deparse_expr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ']');

}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
hdfs_print_remote_param(deparse_expr_cxt *context)
{
	appendStringInfoChar(context->buf, '?');
}

/*
 * Print the representation of a placeholder for a parameter that will be
 * sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query.
 * We don't have the actual value of the runtime parameter yet, and we don't
 * want the remote planner to generate a plan that depends on such a value
 * anyway.  Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of Postgres, the planner will see that as an unknown
 * constant value, which is what we want.  This might need adjustment if we
 * ever make the planner flatten scalar subqueries.  Note: the reason for the
 * apparently useless outer cast is to ensure that the representation as a
 * whole will be parsed as an a_expr and not a select_with_parens; the latter
 * would do the wrong thing in the context "x = ANY(...)".
 */
static void
hdfs_print_remote_placeholder(deparse_expr_cxt *context)
{
	appendStringInfoString(context->buf, "(SELECT null)");
}

/*
 * hdfs_is_subquery_var
 *
 * Returns true if given Var is deparsed as a subquery output column, in
 * which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool
hdfs_is_subquery_var(Var *node, RelOptInfo *foreignrel, int *relno, int *colno,
					 deparse_expr_cxt *context)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;
	RelOptInfo *outerrel = fpinfo->outerrel;
	RelOptInfo *innerrel = fpinfo->innerrel;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel) ||
		   IS_UPPER_REL(foreignrel));

	/*
	 * If the given relation isn't a join relation, it doesn't have any lower
	 * subqueries, so the Var isn't a subquery output column.
	 */
	if (!IS_JOIN_REL(foreignrel))
		return false;

	/*
	 * If the Var doesn't belong to any lower subqueries, it isn't a subquery
	 * output column.
	 */
	if (!bms_is_member(node->varno, fpinfo->lower_subquery_rels))
		return false;

	if (bms_is_member(node->varno, outerrel->relids))
	{
		/*
		 * If outer relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_outerrel_subquery)
		{
			hdfs_get_relation_column_alias_ids(node, outerrel, relno, colno,
											   context);
			return true;
		}

		/* Otherwise, recurse into the outer relation. */
		return hdfs_is_subquery_var(node, outerrel, relno, colno, context);
	}
	else
	{
		Assert(bms_is_member(node->varno, innerrel->relids));

		/*
		 * If inner relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_innerrel_subquery)
		{
			hdfs_get_relation_column_alias_ids(node, innerrel, relno, colno,
											   context);
			return true;
		}

		/* Otherwise, recurse into the inner relation. */
		return hdfs_is_subquery_var(node, innerrel, relno, colno, context);
	}
}

/*
 * hdfs_get_relation_column_alias_ids
 *
 * Get the IDs for the relation and column alias to given Var belonging to
 * given relation, which are returned into *relno and *colno.
 */
static void
hdfs_get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
								   int *relno, int *colno,
								   deparse_expr_cxt *context)
{
	HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) foreignrel->fdw_private;
	int			i;
	ListCell   *lc;
	List	   *scan_var_list = NIL;
	List	   *whole_row_lists = NIL;

	scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
									PVC_RECURSE_PLACEHOLDERS);

	scan_var_list = hdfs_adjust_whole_row_ref(context->root, scan_var_list,
											  &whole_row_lists,
											  foreignrel->relids);
	/* Get the relation alias ID */
	*relno = fpinfo->relation_index;

	/* Get the column alias ID */
	i = 1;
	foreach(lc, scan_var_list)
	{
		if (equal(lfirst(lc), (Node *) node))
		{
			*colno = i;
			return;
		}
		i++;
	}

	/* Shouldn't get here */
	elog(ERROR, "unexpected expression in subquery output");
}

/*
 * hdfs_deparse_aggref
 *		Deparse an Aggref node.
 */
static void
hdfs_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		use_variadic;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Find aggregate name from aggfnoid which is a pg_proc entry. */
	hdfs_append_function_name(node->aggfnoid, context);
	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	/* aggstar can be set only in zero-argument aggregates. */
	if (node->aggstar)
		appendStringInfoChar(buf, '*');
	else
	{
		ListCell   *arg;
		bool		first = true;

		/* Add all the arguments. */
		foreach(arg, node->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(arg);
			Node	   *n = (Node *) tle->expr;

			if (tle->resjunk)
				continue;

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			hdfs_deparse_expr((Expr *) n, context);
		}
	}

	appendStringInfoChar(buf, ')');
}

/*
 * hdfs_append_groupby_clause
 * 		Deparse GROUP BY clause.
 */
static void
hdfs_append_groupby_clause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = context->root->parse;
	ListCell   *lc;
	bool		first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
		return;

	appendStringInfoString(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach(lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		hdfs_deparse_sort_group_clause(grp->tleSortGroupRef, tlist,
									   context);
	}
}

/*
 * hdfs_deparse_sort_group_clause
 * 		Appends a sort or group clause.
 */
static Node *
hdfs_deparse_sort_group_clause(Index ref, List *tlist,
							   deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		hdfs_deparse_const((Const *) expr, context);
	}
	else if (!expr || IsA(expr, Var))
		hdfs_deparse_expr(expr, context);
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		hdfs_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}

/*
 * hdfs_is_foreign_param
 * 		Returns true if given expr is something we'd have to send the
 * 		value of to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * hdfs_deparse_expr would add to context->params_list. Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool
hdfs_is_foreign_param(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var. */
				Var		   *var = (Var *) expr;
				Relids		relids;
				HDFSFdwRelationInfo *fpinfo = (HDFSFdwRelationInfo *) (baserel->fdw_private);

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param. */
				else
					return true;	/* it'd have to be a param. */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server. */
			return true;
		default:
			break;
	}
	return false;
}
