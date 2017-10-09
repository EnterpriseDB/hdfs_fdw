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

/*
 * In order to get number of rows in a hive table we use
 * explain select * from names_tab;
 * It produces a result similar to the following
+--------------------------------------------------------------------------------------------+--+
|                                          Explain                                           |
+--------------------------------------------------------------------------------------------+--+
| STAGE DEPENDENCIES:                                                                        |
|   Stage-0 is a root stage                                                                  |
|                                                                                            |
| STAGE PLANS:                                                                               |
|   Stage: Stage-0                                                                           |
|     Fetch Operator                                                                         |
|       limit: -1                                                                            |
|       Processor Tree:                                                                      |
|         TableScan                                                                          |
|           alias: names_tab                                                                 |
|           Statistics: Num rows: 10 Data size: 36 Basic stats: PARTIAL Column stats: NONE   |
|           Select Operator                                                                  |
|             expressions: id (type: int), name (type: string)                               |
|             outputColumnNames: _col0, _col1                                                |
|             Statistics: Num rows: 10 Data size: 36 Basic stats: PARTIAL Column stats: NONE |
|             ListSink                                                                       |
|                                                                                            |
+--------------------------------------------------------------------------------------------+--+
17 rows selected (0.706 seconds)
 *
 * In order to extract number of rows from this output,
 * each row of explain output is sent one by one to this function.
 * The function tries to find the keyword "Statistics: Num rows:"
 * in each row, if the row does not contain any such keyword
 * it is rejected. Otherwise the characters from the line are
 * picked right after the keyword and till a space is encountered.
 * The characters are then converted to a float, and is used
 * as row count.
 */

double
hdfs_find_row_count(char *src)
{
	char	rc[100];
	int	i;

	memset(rc, 0, 100);
	if (src == NULL || strlen(src) < 80)
		return 0;

	/* does the passed line of explain output contain the keyword? */
	if (strstr(src, "          Statistics: Num rows: ") == NULL)
		return 0;

	/* copy characters after the keyword in the line */
	strncpy(rc, src + 32, 80);

	/* separate the characters till space is found in the line */
	for (i = 0; i < 50; i++)
	{
		if (rc[i] == ' ')
		{
			rc[i] = '\0';
			return atof(rc);
		}
	}
	return 0;
}

double
hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
				RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo)
{
	bool            is_null;
	char            *value = NULL;
	int             idx = 0;
	double          rows = 1000.0;
	StringInfoData  sql;
	int		i;
	double		rc;

	initStringInfo(&sql);
	hdfs_deparse_explain(opt, &sql, root, baserel, fpinfo);
	hdfs_query_execute(con_index, opt, sql.data);

	rc = 0.0;
	while (1)
	{
		if (hdfs_fetch(con_index, opt) == 0)
		{
			value = hdfs_get_field_as_cstring(con_index, opt, idx, &is_null);
			if (!is_null)
			{
				rc = hdfs_find_row_count(value);
				if (rc != 0.0)
				{
					/*
					 * Any value below 1000 is going to caluse materilization
					 * step to be skipped from the plan, which is very
					 * crucial to avoid requery in rescan
					 */
					if (rc > 1000)
						rows = rc;
					break;
				}
			}
		}
		else
		{
			break;
		}
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
