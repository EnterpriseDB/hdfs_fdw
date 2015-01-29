/*-------------------------------------------------------------------------
 *
 * hdfs_client.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_client.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libhive/odbc/hiveclient.h"

#include "hdfs_fdw.h"

HiveReturn
hdfs_fetch(hdfs_opt *opt, HiveResultSet *rs)
{
	HiveReturn r;
	char err_buf[512];
	r = DBFetch(rs, err_buf, sizeof(err_buf));
	if (r == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch data from HiveServer: %s", err_buf)));
	return r;
}

unsigned int
hdfs_get_column_count(hdfs_opt *opt, HiveResultSet *rs)
{
	unsigned int count = 0;
	char err_buf[512];
	if (DBGetColumnCount(rs, &count, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to get column count HiveServer: %s", err_buf)));
	return count;
}

unsigned int
hdfs_get_field_data_len(hdfs_opt *opt, HiveResultSet *rs, int col)
{
	unsigned int len;
	char err_buf[512];
	if (DBGetFieldDataLen(rs, col, &len, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch field length from HiveServer: %s", err_buf)));
	return len;
}

char *
hdfs_get_field_as_cstring(hdfs_opt *opt, HiveResultSet *rs, int idx, bool *is_null, int len)
{
	unsigned int   bs;
	char           *value;
	int            isnull = 0;
	char err_buf[512];
			
	value = (char*)palloc(len);
	if (DBGetFieldAsCString(rs, idx, value, len, &bs, &isnull, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("failed to fetch field from HiveServer: %s", err_buf)));
	if (isnull == 0)
		*is_null = false;
	else
		*is_null = true;
	return value;
}

HiveResultSet*
hdfs_query_execute(hdfs_opt *opt, char *query)
{
	HiveResultSet *rs;
	char  err_buf[512];
	if (DBExecute(opt->conn, query, &rs, 1000, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch execute query: %s", err_buf)));
	return rs;
}

void
hdfs_close_result_set(hdfs_opt *opt, HiveResultSet *rs)
{
	char  err_buf[512];
	DBCloseResultSet(rs, err_buf, sizeof(err_buf));
}
