/*-------------------------------------------------------------------------
 *
 * hdfs_query.c
 * 		Planner helper functions to get information from Hive/Spark server.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hdfs_fdw.h"
#include "libhive/jdbc/hiveclient.h"

static double hdfs_find_row_count(char *src);

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
 * In order to extract number of rows from this output, each row of explain
 * output is sent one by one to this function. The function tries to find the
 * keyword "Statistics: Num rows:" in each row, if the row does not contain any
 * such keyword it is rejected. Otherwise the characters from the line are
 * picked right after the keyword and till a space is encountered. The
 * characters are then converted to a float, and is used as row count.
 */
double
hdfs_find_row_count(char *src)
{
	char		row_count[51];
	char		statistics_str[] = "Statistics: Num rows: ";
	char	   *pos;

	if (src == NULL || strlen(src) < 80)
		return 0;

	/* Does the passed line of explain output contain the keyword? */
	pos = strstr(src, statistics_str);
	if (pos == NULL)
		return 0;

	/* Copy characters after the keyword in the line */
	strncpy(row_count, pos + strlen(statistics_str), 50);

	/* Make sure that the string is null terminated */
	row_count[51] = '\0';

	return strtod(row_count, NULL);
}

/*
 * hdfs_rowcount
 * 		Retrieves the number of rows from remote server. Refer the comments
 * 		above function hdfs_find_row_count() for how it is done.
 */
double
hdfs_rowcount(int con_index, hdfs_opt *opt, PlannerInfo *root,
			  RelOptInfo *baserel, HDFSFdwRelationInfo *fpinfo)
{
	bool		is_null;
	StringInfoData sql;
	double		rc;

	initStringInfo(&sql);
	hdfs_deparse_explain(opt, &sql);
	hdfs_query_execute(con_index, opt, sql.data);

	while (hdfs_fetch(con_index) == 0)
	{
		char	   *value;

		value = hdfs_get_field_as_cstring(con_index, 0, &is_null);

		if (!is_null)
		{
			rc = hdfs_find_row_count(value);

			if (rc != 0.0)
				break;
		}
	}

	hdfs_close_result_set(con_index);

	/*
	 * Any value below 1000 is going to cause materialization step to be
	 * skipped from the plan, which is very crucial to avoid requery in rescan.
	 */
	return (rc > 1000) ? rc : 1000;
}

/*
 * hdfs_analyze
 * 		Sends analyze query to remote server to compute the statistics.
 */
void
hdfs_analyze(int con_index, hdfs_opt *opt, Relation rel)
{
	StringInfoData sql;

	initStringInfo(&sql);
	hdfs_deparse_analyze(&sql, rel);

	if (opt->client_type == SPARKSERVER)
		hdfs_query_execute(con_index, opt, sql.data);
	else
		hdfs_query_execute_utility(con_index, opt, sql.data);

	hdfs_close_result_set(con_index);
}

/*
 * hdfs_describe
 * 		This function sends describe query to the remote server and retrieves
 * 		the total size of the data in remote table.
 */
double
hdfs_describe(int con_index, hdfs_opt *opt, Relation rel)
{
	double		row_count = 0;
	StringInfoData sql;

	initStringInfo(&sql);
	hdfs_deparse_describe(&sql, rel);
	hdfs_query_execute(con_index, opt, sql.data);

	/*
	 * hdfs_deparse_describe() sends a query of the form "DESCRIBE FORMATTED
	 * sometab" to the remote server.  This produces the output in the columnar
	 * format.  The 'totalSize' is placed in the 1st column (indexed by 0) and
	 * its value is placed in the 2nd column of the same row.  Hence, we
	 * directly search for the 1st column of each row until we find the
	 * 'totalSize', and once we find that, only then we retrieve the 2nd column
	 * of that row and break.
	 */
	while (hdfs_fetch(con_index) == 0)
	{
		char   *value;
		bool	is_null;

		value = hdfs_get_field_as_cstring(con_index, 1, &is_null);

		if (is_null)
			continue;

		if (strstr(value, "totalSize") != 0)
		{
			char   *str;

			str = hdfs_get_field_as_cstring(con_index, 2, &is_null);
			row_count = strtod(str, NULL);

			break;
		}
	}

	hdfs_close_result_set(con_index);
	return row_count;
}
