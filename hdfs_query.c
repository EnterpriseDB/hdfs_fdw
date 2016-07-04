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

#include "libhive/odbc/hiveclient.h"
#include "hdfs_fdw.h"

double
hdfs_rowcount(HiveConnection *conn, hdfs_opt *opt, PlannerInfo *root, RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo)
{
	HiveResultSet*  rs = NULL;
	size_t          len = 0;
	bool            is_null;
	char            *value = NULL;
	int             idx = 0;
	double          rows = 0;
	StringInfoData  sql;

	initStringInfo(&sql);
	hdfs_deparse_explain(opt, &sql, root, baserel, fpinfo);

	rs = hdfs_query_execute(conn, opt, sql.data);
	if (hdfs_fetch(opt, rs) == HIVE_SUCCESS)
	{
		len = hdfs_get_field_data_len(opt, rs, idx);
		value = hdfs_get_field_as_cstring(opt, rs, idx, &is_null, len + 1);
		if (!is_null)
			rows = atof(value);
		pfree(value);
	}
	hdfs_close_result_set(opt, rs);
	return rows;
}

void
hdfs_analyze(HiveConnection *conn, hdfs_opt *opt)
{
	HiveResultSet*  rs = NULL;
	StringInfoData  sql;

	initStringInfo(&sql);
	hdfs_deparse_analyze(&sql, opt);
	rs = hdfs_query_execute(conn, opt, sql.data);
	hdfs_close_result_set(opt, rs);
}

List *
hdfs_desc_query(HiveConnection *conn, hdfs_opt *opt)
{
	HiveResultSet*  rs = NULL;
	size_t          len;
	char            *value;
	double          rows = 0;
	StringInfoData  sql;
	int             i = 0;
	int             count;
	bool            is_null;
	int             found = 0;
	List            *col_list = NULL;
	hdfs_column     *cols;
	char            *col_type;

	initStringInfo(&sql);

	appendStringInfo(&sql, "DESC %s", opt->table_name);
	rs = hdfs_query_execute(conn, opt, sql.data);

	elog(DEBUG1, "Remote Describe SQL: %s", sql.data);

	while (hdfs_fetch(opt, rs) == HIVE_SUCCESS)
	{
		cols = palloc0(sizeof(hdfs_column));
		len = hdfs_get_field_data_len(opt, rs, 0);
		if (len > 0)
		{
			cols->col_name = hdfs_get_field_as_cstring(opt, rs, 0, &is_null, len + 1);
			if (is_null)
				continue;
		}
		len = hdfs_get_field_data_len(opt, rs, 1);
		if (len > 0)
		{
			col_type = hdfs_get_field_as_cstring(opt, rs, 1, &is_null, len + 1);
			if (is_null)
				continue;
		}

		if (strcasecmp(col_type, "TINYINT") == 0)
			cols->col_type = HDFS_TINYINT;

		else if (strcasecmp(col_type, "SMALLINT") == 0)
			cols->col_type = HDFS_SMALLINT;

		else if (strcasecmp(col_type, "INT") == 0)
			cols->col_type = HDFS_INT;

		else if (strcasecmp(col_type, "BIGINT") == 0)
			cols->col_type = HDFS_BIGINT;

		else if (strcasecmp(col_type, "String") == 0)
			cols->col_type = HDFS_STRING;

		else if (strcasecmp(col_type, "Varchar") == 0)
			cols->col_type = HDFS_VARCHAR;

		else if (strcasecmp(col_type, "CHAR") == 0)
			cols->col_type = HDFS_CHAR;

		else if (strcasecmp(col_type, "Timestamps") == 0)
			cols->col_type = HDFS_TIMESTAMPS;

		else if (strcasecmp(col_type, "Dacimal") == 0)
			cols->col_type = HDFS_DACIMAL;

		else if (strcasecmp(col_type, "Date") == 0)
			cols->col_type = HDFS_DATE;

		col_list = lappend(col_list, cols);
	}
	hdfs_close_result_set(opt, rs);
	return col_list;
}


double
hdfs_describe(HiveConnection *conn, hdfs_opt *opt)
{
	HiveResultSet*  rs = NULL;
	size_t          len;
	char            *value;
	double          rows = 0;
	StringInfoData  sql;
	int             i = 0;
	int             count;
	bool            is_null;
	int             found = 0;

	initStringInfo(&sql);
	hdfs_deparse_describe(&sql, opt);
	rs = hdfs_query_execute(conn, opt, sql.data);
	while (hdfs_fetch(opt, rs) == HIVE_SUCCESS)
	{
		count = hdfs_get_column_count(opt, rs);
		for (i = 0; i < count; i++)
		{
			len = hdfs_get_field_data_len(opt, rs, i);
			if (len <= 1)
				continue;
			
			value = hdfs_get_field_as_cstring(opt, rs, i, &is_null, len + 1);
			if (is_null)
				continue;

			if (found == 1)
			{
				rows = (double)strtoul(value, NULL, 10);
				hdfs_close_result_set(opt, rs);
				return rows;
			}
			if (strstr(value, "totalSize") != 0)
				found = 1;
		}
	}
	hdfs_close_result_set(opt, rs);
	return rows;
}
