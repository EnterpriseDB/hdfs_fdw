/*-------------------------------------------------------------------------
 *
 * hdfs_connection.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "hdfs_fdw.h"

#include "access/xact.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

int
hdfs_get_connection(ForeignServer *server, UserMapping *user, hdfs_opt *opt)
{
	int		conn;
	char	*err = "unknown";
	char	*err_buf = err;

	conn = DBOpenConnection(opt->host,
							opt->port,
							opt->dbname,
							opt->username,
							opt->password,
							opt->principal,
							opt->connect_timeout,
							opt->receive_timeout,
							opt->auth_type,
							opt->client_type,
							&err_buf);
	if (conn < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				errmsg("failed to initialize the HDFS connection object (%s)", err_buf)));

	ereport(DEBUG1, (errmsg("hdfs_fdw: connection opened %d", conn)));

	return conn;
}

/*
 * Release connection created by calling GetConnection.
 */
void
hdfs_rel_connection(int con_index)
{
	int		r;

	r = DBCloseConnection(con_index);
	if (r < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				errmsg("failed to close HDFS connection object")));

	ereport(DEBUG1, (errmsg("hdfs_fdw: connection closed %d", con_index)));
}
