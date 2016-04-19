/*-------------------------------------------------------------------------
 *
 * hdfs_connection.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
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

HiveConnection*
hdfs_get_connection(ForeignServer *server, UserMapping *user, hdfs_opt *opt)
{
	HiveConnection *conn = NULL;
	char           err_buf[512];

	conn = DBOpenConnection(opt->dbname,
				opt->host,
				opt->port,
				0,
				opt->client_type,
				opt->connect_timeout,
				opt->receive_timeout,
				err_buf,
				512);
	if (!conn)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				errmsg("failed to initialize the HDFS connection object (%s)", err_buf)));

	return conn;
}

/*
 * Release connection created by calling GetConnection.
 */
void
hdfs_rel_connection(HiveConnection *conn)
{
	char err_buf[512];
	HiveReturn r;
	r = DBCloseConnection(conn,
			      err_buf,
			      512);
	if (r == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				errmsg("failed to close HDFS connection object (%s)", err_buf)));
}
