/*-------------------------------------------------------------------------
 *
 * hdfs_query.c
 * 		Foreign-data wrapper for remote HDFS servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libhive/jdbc/hiveclient.h"
#include "hdfs_fdw.h"

double
hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
				RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo)
{
	bool            is_null;
	char            *value = NULL;
	int             idx = 0;
	double          rows = 0;
	StringInfoData  sql;

	initStringInfo(&sql);
	hdfs_deparse_explain(opt, &sql, root, baserel, fpinfo);

	hdfs_query_execute(con_index, opt, sql.data);
	if (hdfs_fetch(con_index, opt) == 0)
	{
		value = hdfs_get_field_as_cstring(con_index, opt, idx, &is_null);
		if (!is_null)
			rows = atof(value);
		pfree(value);
	}
	hdfs_close_result_set(con_index, opt);
	return rows;
}

void
hdfs_analyze(int con_index, hdfs_opt *opt)
{
	StringInfoData  sql;

	initStringInfo(&sql);
	hdfs_deparse_analyze(&sql, opt);
	if (opt->client_type == SPARKSERVER)
		hdfs_query_execute(con_index, opt, sql.data);
	else
		hdfs_query_execute_utility(con_index, opt, sql.data);
	hdfs_close_result_set(con_index, opt);
}

List *
hdfs_desc_query(int con_index, hdfs_opt *opt)
{
	char            *value;
	double          rows = 0;
	StringInfoData  sql;
	int             i = 0;
	int             count;
	bool            is_null;
	int             found = 0;
	List            *col_list = NULL;
	hdfs_column     *cols;
	char            *col_type = NULL;

	initStringInfo(&sql);

	appendStringInfo(&sql, "DESC %s", opt->table_name);
	hdfs_query_execute(con_index, opt, sql.data);

	elog(DEBUG1, "Remote Describe SQL: %s", sql.data);

	while (hdfs_fetch(con_index, opt) == 0)
	{
		cols = palloc0(sizeof(hdfs_column));
		cols->col_name = hdfs_get_field_as_cstring(con_index, opt, 0, &is_null);
		if (is_null)
			continue;
		col_type = hdfs_get_field_as_cstring(con_index, opt, 1, &is_null);
		if (col_type == NULL || is_null)
			continue;

		if (strcasecmp(col_type, "TINYINT") == 0)
			cols->col_type = HDFS_TINYINT;

		else if (strcasecmp(col_type, "SMALLINT") == 0)
			cols->col_type = HDFS_SMALLINT;

		else if (strcasecmp(col_type, "INT") == 0)
			cols->col_type = HDFS_INT;

		else if (strcasecmp(col_type, "double") == 0)
			cols->col_type = HDFS_DOUBLE;

		else if (strcasecmp(col_type, "BIGINT") == 0)
			cols->col_type = HDFS_BIGINT;

		else if (strcasecmp(col_type, "Boolean") == 0)
			cols->col_type = HDFS_BOLEAN;

		else if (strcasecmp(col_type, "String") == 0)
			cols->col_type = HDFS_STRING;

		else if (strcasecmp(col_type, "Varchar") == 0)
			cols->col_type = HDFS_VARCHAR;

		else if (strcasecmp(col_type, "CHAR") == 0)
			cols->col_type = HDFS_CHAR;

		else if (strstr(col_type, "char") != 0)
			cols->col_type = HDFS_CHAR;

		else if (strcasecmp(col_type, "Timestamps") == 0)
			cols->col_type = HDFS_TIMESTAMPS;

		else if (strcasecmp(col_type, "timestamp") == 0)
			cols->col_type = HDFS_TIMESTAMPS;

		else if (strcasecmp(col_type, "Decimal") == 0)
			cols->col_type = HDFS_DECIMAL;

		else if (strstr(col_type, "decimal") != 0)
			cols->col_type = HDFS_DECIMAL;

		else if (strcasecmp(col_type, "Date") == 0)
			cols->col_type = HDFS_DATE;

		else if (strstr(col_type, "varchar") != 0)
			cols->col_type = HDFS_VARCHAR;

		else
		{
			hdfs_close_result_set(con_index, opt);
			hdfs_rel_connection(con_index);

			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg("unsupported Hive data type"),
					errhint("Supported data types are TINYINT, SMALLINT, INT, BIGINT, BOOLEAN, DOUBLE, STRING, CHAR, TIMESTAMP, DECIMAL, DATE and VARCHAR: %s", col_type)));

		}
		col_list = lappend(col_list, cols);
	}
	hdfs_close_result_set(con_index, opt);
	return col_list;
}


double
hdfs_describe(int con_index, hdfs_opt *opt)
{
	char            *value;
	double          rows = 0;
	StringInfoData  sql;
	int             i = 0;
	int             count;
	bool            is_null;
	int             found = 0;

	initStringInfo(&sql);
	hdfs_deparse_describe(&sql, opt);
	hdfs_query_execute(con_index, opt, sql.data);
	while (hdfs_fetch(con_index, opt) == 0)
	{
		count = hdfs_get_column_count(con_index, opt);
		for (i = 0; i < count; i++)
		{
			value = hdfs_get_field_as_cstring(con_index, opt, i, &is_null);
			if (is_null)
				continue;

			if (found == 1)
			{
				rows = (double)strtoul(value, NULL, 10);
				hdfs_close_result_set(con_index, opt);
				return rows;
			}
			if (strstr(value, "totalSize") != 0)
				found = 1;
		}
	}
	hdfs_close_result_set(con_index, opt);
	return rows;
}
