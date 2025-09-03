/*-------------------------------------------------------------------------
 *
 * hdfs_connection.c
 * 		Connection management functions.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hdfs_fdw.h"

/*
 * hdfs_get_connection
 * 		Creates a Hive/Spark server connection.
 */
int
hdfs_get_connection(ForeignServer *server, hdfs_opt *opt)
{
	int			conn;
	char	   *err_buf = "unknown";
	StringInfoData connstr;

	initStringInfo(&connstr);
	appendStringInfo(&connstr, "jdbc:hive2://%s:%d/%s",
					 opt->host, opt->port, opt->dbname);
	if (opt->ssl)
		appendStringInfo(&connstr, ";ssl=true");
	if (opt->useSystemTrustStore)
		appendStringInfo(&connstr, ";useSystemTrustStore=true");
	if (opt->sslTrustStore)
		appendStringInfo(&connstr, ";sslTrustStore=%s", opt->sslTrustStore);
	if (opt->trustStorePassword)
		appendStringInfo(&connstr, ";trustStorePassword=%s",
						 opt->trustStorePassword);
	if (opt->auth_type_str)
		appendStringInfo(&connstr, ";auth=%s", opt->auth_type_str);
	else
	{
		if (opt->username == NULL || opt->username[0] == '\0')
			appendStringInfo(&connstr, ";auth=noSasl");	/* DEFAULT */
	}

	elog(DEBUG3, "connection string: %s", connstr.data);

	conn = DBOpenConnection(opt->host,
							opt->port,
							opt->username,
							opt->password,
							connstr.data,
							opt->connect_timeout,
							opt->receive_timeout,
							opt->auth_type,
							opt->client_type,
							&err_buf);
	if (conn < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to initialize the connection: (%s)",
						err_buf)));

	ereport(DEBUG1,
			(errmsg("hdfs_fdw: new connection(%d) opened for server \"%s\"",
					conn, server->servername)));

	return conn;
}

/*
 * hdfs_rel_connection
 * 		Release connection created by hdfs_get_connection.
 */
void
hdfs_rel_connection(int con_index)
{
	int			r;

	r = DBCloseConnection(con_index);
	if (r < 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to close the connection(%d)", con_index)));

	ereport(DEBUG1,
			(errmsg("hdfs_fdw: connection(%d) closed", con_index)));
}
