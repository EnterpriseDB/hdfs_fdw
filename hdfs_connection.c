/*-------------------------------------------------------------------------
 *
 * hdfs_connection.c
 * 		Connection management functions.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2026, EnterpriseDB Corporation.
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

	/*
	 * Log connection string with the trustStorePassword value redacted.  A
	 * fixed-width mask is used so the password length is not revealed.
	 */
	{
		char	   *pw_start = strstr(connstr.data, "trustStorePassword=");

		if (pw_start)
		{
			StringInfoData redacted;
			char	   *pw_val = pw_start + strlen("trustStorePassword=");
			char	   *pw_end = strchr(pw_val, ';');

			initStringInfo(&redacted);
			/* copy everything up to and including "trustStorePassword=" */
			appendBinaryStringInfo(&redacted, connstr.data, pw_val - connstr.data);
			appendStringInfoString(&redacted, "****");
			/* append the remainder following the password value, if any */
			if (pw_end)
				appendStringInfoString(&redacted, pw_end);

			elog(DEBUG3, "connection string: %s", redacted.data);
			pfree(redacted.data);
		}
		else
			elog(DEBUG3, "connection string: %s", connstr.data);
	}

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

	/* Scrub the cleartext connection string from memory. */
	explicit_bzero(connstr.data, connstr.len);
	pfree(connstr.data);

	if (conn < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to initialize the connection: (%s)",
						err_buf)));

	/*
	 * Zero out sensitive credentials now that connection is established.
	 * We don't pfree here because these strings point into DefElem nodes
	 * that may still be referenced; the memory context will reclaim them.
	 */
	if (opt->password)
	{
		explicit_bzero(opt->password, strlen(opt->password));
		opt->password = NULL;
	}
	if (opt->trustStorePassword)
	{
		explicit_bzero(opt->trustStorePassword, strlen(opt->trustStorePassword));
		opt->trustStorePassword = NULL;
	}

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
