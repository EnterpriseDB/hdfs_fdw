/*-------------------------------------------------------------------------
 *
 * hdfs_option.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hdfs_fdw.h"

#include "libhive/jdbc/hiveclient.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct HDFSFdwOption
{
	const char       *optname;     /* Name of the options */
	Oid              optcontext;   /* Oid of catalog in which option may appear */
};


/*
 * Valid options for hdfs_fdw.
 *
 */
static struct HDFSFdwOption valid_options[] =
{
	/* Connection options */
	{ "host",                ForeignServerRelationId },
	{ "port",                ForeignServerRelationId },
	{ "username",            UserMappingRelationId },
	{ "password",            UserMappingRelationId },
	{ "principal",           ForeignServerRelationId },
	{ "dbname",              ForeignTableRelationId },
	{ "table_name",          ForeignTableRelationId },
	{ "client_type",         ForeignServerRelationId },
	{ "auth_type",           ForeignServerRelationId },
	{ "use_remote_estimate", ForeignServerRelationId },
	{ "query_timeout",       ForeignServerRelationId },
	{ "connect_timeout",     ForeignServerRelationId },
	{ "fetch_size",          ForeignServerRelationId },
	{ "log_remote_sql",      ForeignServerRelationId },
	{ NULL,                  InvalidOid }
};

extern Datum hdfs_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hdfs_fdw_validator);

static bool hdfs_is_valid_option(const char *option, Oid context);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
hdfs_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell	*cell;

	/*
	 * Check that only options supported by hdfs_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	 *def = (DefElem *) lfirst(cell);

		if (!hdfs_is_valid_option(def->defname, catalog))
		{
			struct HDFSFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
			}

			ereport(ERROR, 
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
				errmsg("invalid option \"%s\"", def->defname), 
				errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
				));
		}
	}
	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
hdfs_is_valid_option(const char *option, Oid context)
{
	struct HDFSFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a hdfs_fdw foreign table.
 */
hdfs_opt*
hdfs_get_options(Oid foreigntableid)
{
	ForeignTable        *f_table;
	ForeignServer       *f_server;
	UserMapping         *f_mapping;
	List                *options;
	ListCell            *lc;
	hdfs_opt            *opt;
	char                *table_name = NULL;

	opt = (hdfs_opt*) palloc(sizeof(hdfs_opt));
	memset(opt, 0, sizeof(hdfs_opt));

	opt->receive_timeout = 1000 * 300;  /* Default timeout is 300 seconds */
	opt->connect_timeout = 1000 * 300;  /* Default timeout is 300 seconds */
	opt->fetch_size = 10000;			/* Default fetch size */
	opt->log_remote_sql = false;

	/*
	 * Extract options from FDW objects.
	 */
	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);
	f_mapping = GetUserMapping(GetUserId(), f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Set default clinet type to HiverServer2 */
	opt->client_type = HIVESERVER2;

	opt->auth_type = AUTH_TYPE_UNSPECIFIED;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "host") == 0)
			opt->host = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
		{
			opt->port = atoi(defGetString(def));
			if (opt->port <= 0 || opt->port >= 65535)
				elog(ERROR, "invalid port number: %s", defGetString(def));
		}

		if (strcmp(def->defname, "username") == 0)
			opt->username = defGetString(def);

		if (strcmp(def->defname, "password") == 0)
			opt->password = defGetString(def);

		if (strcmp(def->defname, "principal") == 0)
			opt->principal = defGetString(def);

		if (strcmp(def->defname, "dbname") == 0)
			opt->dbname = defGetString(def);

		if (strcmp(def->defname, "table_name") == 0)
			table_name = defGetString(def);

		if (strcmp(def->defname, "client_type") == 0)
		{
			if (strcasecmp(defGetString(def), "hiveserver2") == 0)
				opt->client_type = HIVESERVER2;
			else
			{
				if (strcasecmp(defGetString(def), "spark") == 0)
					opt->client_type = SPARKSERVER;
				else
				{
					ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", defGetString(def)),
								errhint("Valid client_type values are hiveserver2 and spark")));
				}
			}
		}

		if (strcmp(def->defname, "auth_type") == 0)
		{
			if (strcasecmp(defGetString(def), "NOSASL") == 0)
				opt->auth_type = AUTH_TYPE_NOSASL;
			else
			{
				if (strcasecmp(defGetString(def), "LDAP") == 0)
					opt->auth_type = AUTH_TYPE_LDAP;
				else {
					if (strcasecmp(defGetString(def), "KERBEROS") == 0)
						opt->auth_type = AUTH_TYPE_KERBEROS;
					else
						ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
								errmsg("invalid option \"%s\"", defGetString(def)),
									errhint("Valid auth_type are NOSASL & LDAP & KERBEROS")));
				}
			}
		}

		if (strcmp(def->defname, "log_remote_sql") == 0)
			opt->log_remote_sql = defGetBoolean(def);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			opt->use_remote_estimate = defGetBoolean(def);

		if (strcmp(def->defname, "fetch_size") == 0)
			opt->fetch_size = atoi(defGetString(def));

		if (strcmp(def->defname, "query_timeout") == 0)
		{
			opt->receive_timeout = atoi(defGetString(def));
			if (opt->receive_timeout <= 0 || opt->receive_timeout >= 100000)
				ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid query timeout \"%s\"", defGetString(def)),
							errhint("Valid range is 1 - 100000 S")));
			opt->receive_timeout = opt->receive_timeout * 1000;
		}
		if (strcmp(def->defname, "connect_timeout") == 0)
		{
			opt->connect_timeout = atoi(defGetString(def));
			if (opt->connect_timeout <= 0 || opt->connect_timeout >= 100000)
				ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid connect timeout \"%s\"", defGetString(def)),
							errhint("Valid range is 1 - 100000 S")));
			opt->connect_timeout = opt->connect_timeout * 1000;	
		}
	}

	/* Default values, if required */
	if (!opt->host)
		opt->host = (char*)DEFAULT_HOST;

	if (!opt->port)
		opt->port = atoi(DEFAULT_PORT);

	if (!opt->dbname)
		opt->dbname = (char*)DEFAULT_DATABASE;

	if (!table_name)
		table_name = get_rel_name(foreigntableid);

	opt->table_name = palloc(strlen(table_name) + strlen(opt->dbname) + 2);
	sprintf(opt->table_name, "%s.%s", opt->dbname, table_name);

	return opt;
}


