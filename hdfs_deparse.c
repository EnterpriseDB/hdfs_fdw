/*-------------------------------------------------------------------------
 *
 * hdfs_deparse.c
 * 		Query deparser for hdfs_fdw.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
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
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "hdfs_fdw.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/clauses.h"
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
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
} deparse_expr_cxt;

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
static void hdfs_deparse_column_ref(StringInfo buf, int varno, int varattno,
									PlannerInfo *root);
static void hdfs_deparse_relation(hdfs_opt *opt, StringInfo buf);
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

		if (hdfs_is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
hdfs_is_foreign_expr(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
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
	foreign_loc_cxt inner_cxt;
	FDWCollateState state;

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

				if (bms_is_member(var->varno, glob_cxt->foreignrel->relids) &&
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
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

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
	appendStringInfo(buf, "EXPLAIN SELECT * FROM %s", opt->table_name);

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
hdfs_deparse_describe(StringInfo buf, hdfs_opt *opt)
{
	appendStringInfo(buf, "DESCRIBE FORMATTED %s", opt->table_name);
}

void
hdfs_deparse_analyze(StringInfo buf, hdfs_opt *opt)
{
	appendStringInfo(buf, "ANALYZE TABLE %s COMPUTE STATISTICS",
					 opt->table_name);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... FROM tablename".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
void
hdfs_deparse_select(hdfs_opt *opt,
					StringInfo buf,
					PlannerInfo *root,
					RelOptInfo *baserel,
					Bitmapset *attrs_used,
					List **retrieved_attrs)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
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

	/* Construct SELECT list */
	appendStringInfoString(buf, "SELECT ");
	hdfs_deparse_target_list(buf, root, baserel->relid, rel, attrs_used,
							 retrieved_attrs);

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	hdfs_deparse_relation(opt, buf);

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif
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

	if (attrs_used != NULL &&
		tupdesc->natts == bms_num_members(attrs_used))
	{
		have_wholerow = true;

		/* Send SELECT * query to avoid Map-Reduce job */
		appendStringInfoChar(buf, '*');
	}

	if (bms_num_members(attrs_used) == 0)
		appendStringInfoChar(buf, '*');

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
				hdfs_deparse_column_ref(buf, rtindex, i, root);
			}

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}
}

/*
 * Deparse WHERE clauses in given list of RestrictInfos and append them to buf.
 *
 * baserel is the foreign table we're planning for.
 *
 * If no WHERE clause already exists in the buffer, is_first should be true.
 *
 * If params is not NULL, it receives a list of Params and other-relation Vars
 * used in the clauses; these values must be transmitted to the remote server
 * as parameter values.
 *
 * If params is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 */
void
hdfs_append_where_clause(hdfs_opt *opt, StringInfo buf,
						 PlannerInfo *root,
						 RelOptInfo *baserel,
						 List *exprs,
						 bool is_first,
						 List **params)
{
	deparse_expr_cxt context;
	ListCell   *lc;

	if (params)
		*params = NIL;			/* initialize result list to empty */

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = baserel;
	context.buf = buf;
	context.params_list = params;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
			appendStringInfoString(buf, " WHERE ");
		else
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		hdfs_deparse_expr(ri->clause, &context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
hdfs_deparse_column_ref(StringInfo buf, int varno, int varattno,
						PlannerInfo *root)
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

	appendStringInfoString(buf, quote_identifier(colname));
}

static void
hdfs_deparse_relation(hdfs_opt *opt, StringInfo buf)
{
	appendStringInfo(buf, "%s", opt->table_name);
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
	StringInfo	buf = context->buf;

	if (node->varno == context->foreignrel->relid &&
		node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		hdfs_deparse_column_ref(buf, node->varno, node->varattno,
								context->root);
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
	bool		use_variadic;
	bool		first;
	ListCell   *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		hdfs_deparse_expr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * If the function call came from a cast, then show the first argument
	 * plus an explicit cast operation.
	 */
	if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		int32		coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

		hdfs_deparse_expr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->funcvariadic;

	/* Print schema name only if it's not pg_catalog */
	if (procform->pronamespace != PG_CATALOG_NAMESPACE)
	{
		const char *schemaname;

		schemaname = get_namespace_name(procform->pronamespace);
		appendStringInfo(buf, "%s.", quote_identifier(schemaname));
	}

	/* Deparse the function name ... */
	proname = NameStr(procform->proname);
	appendStringInfo(buf, "%s(", quote_identifier(proname));

	/* Deparse all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
#if PG_VERSION_NUM < 130000
		if (use_variadic && lnext(arg) == NULL)
#else
		if (use_variadic && lnext(node->args, arg) == NULL)
#endif
			appendStringInfoString(buf, "VARIADIC ");
		hdfs_deparse_expr((Expr *) lfirst(arg), context);
		first = false;
	}

	appendStringInfoChar(buf, ')');

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
