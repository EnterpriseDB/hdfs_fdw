/*-------------------------------------------------------------------------
 *
 * hdfs_option.c
 * 		FDW option handling for hdfs_fdw
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "hdfs_fdw.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"

/* Default connection parameters */
#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT 10000

/*
 * Describes the valid options for objects that this wrapper uses.
 */
struct HDFSFdwOption
{
	const char *optname;		/* name of the options */
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};


/*
 * Valid options for hdfs_fdw.
 */
static struct HDFSFdwOption valid_options[] =
{
	/* Connection options */
	{"host", ForeignServerRelationId},
	{"port", ForeignServerRelationId},
	{"username", UserMappingRelationId},
	{"password", UserMappingRelationId},
	{"dbname", ForeignTableRelationId},
	{"table_name", ForeignTableRelationId},
	{"client_type", ForeignServerRelationId},
	{"auth_type", ForeignServerRelationId},
	{"use_remote_estimate", ForeignServerRelationId},
	{"query_timeout", ForeignServerRelationId},
	{"connect_timeout", ForeignServerRelationId},
	{"fetch_size", ForeignServerRelationId},
	{"log_remote_sql", ForeignServerRelationId},
	{"enable_join_pushdown", ForeignServerRelationId},
	{"enable_join_pushdown", ForeignTableRelationId},
	{"enable_aggregate_pushdown", ForeignServerRelationId},
	{"enable_aggregate_pushdown", ForeignTableRelationId},
	{"enable_order_by_pushdown", ForeignServerRelationId},
	{"enable_order_by_pushdown", ForeignTableRelationId},
	{NULL, InvalidOid}
};


static bool hdfs_is_valid_option(const char *option, Oid context);


/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses hdfs_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
PG_FUNCTION_INFO_V1(hdfs_fdw_validator);

Datum
hdfs_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * Check that only options supported by hdfs_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/*
		 * If an unknown option specified, complain about it. Provide a hint
		 * with list of valid options for the object.
		 */
		if (!hdfs_is_valid_option(def->defname, catalog))
		{
			struct HDFSFdwOption *opt;
			StringInfoData buf;

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
					 errhint("Valid options in this context are: %s.",
							 buf.len ? buf.data : "<none>")));
		}

		if (strcmp(def->defname, "enable_join_pushdown") == 0 ||
			strcmp(def->defname, "enable_aggregate_pushdown") == 0 ||
			strcmp(def->defname, "enable_order_by_pushdown") == 0)
			(void) defGetBoolean(def);
	}

	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid hdfs_fdw options.
 * Context is the Oid of the catalog holding the object the option is for.
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
hdfs_opt *
hdfs_get_options(Oid foreigntableid)
{
	ForeignTable *f_table;
	ForeignServer *f_server;
	UserMapping *f_mapping;
	List	   *options;
	ListCell   *lc;
	hdfs_opt   *opt;

	opt = (hdfs_opt *) palloc0(sizeof(hdfs_opt));

	/* Set default values for options. */
	opt->receive_timeout = 1000 * 300;
	opt->connect_timeout = 1000 * 300;
	opt->fetch_size = 10000;
	opt->log_remote_sql = false;
	opt->host = DEFAULT_HOST;
	opt->port = DEFAULT_PORT;
	opt->dbname = DEFAULT_DATABASE;
	opt->enable_join_pushdown = true;
	opt->enable_aggregate_pushdown = true;
	opt->enable_order_by_pushdown = true;

	/* Extract options from FDW objects. */
	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);
	f_mapping = GetUserMapping(GetUserId(), f_table->serverid);

	options = NIL;
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_mapping->options);

	/* Set default client type to HiverServer2 and auth type to unspecified. */
	opt->client_type = HIVESERVER2;
	opt->auth_type = AUTH_TYPE_UNSPECIFIED;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "host") == 0)
			opt->host = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
		{
			opt->port = atoi(defGetString(def));
			if (opt->port <= 0 || opt->port >= 65535)
				ereport(ERROR,
						(errmsg("invalid port number: %s",
								defGetString(def))));
		}

		if (strcmp(def->defname, "username") == 0)
			opt->username = defGetString(def);

		if (strcmp(def->defname, "password") == 0)
			opt->password = defGetString(def);

		if (strcmp(def->defname, "dbname") == 0)
			opt->dbname = defGetString(def);

		if (strcmp(def->defname, "table_name") == 0)
			opt->table_name = defGetString(def);

		if (strcmp(def->defname, "client_type") == 0)
		{
			if (strcasecmp(defGetString(def), "hiveserver2") == 0)
				opt->client_type = HIVESERVER2;
			else if (strcasecmp(defGetString(def), "spark") == 0)
				opt->client_type = SPARKSERVER;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\"", defGetString(def)),
						 errhint("Valid client_type values are hiveserver2 and spark.")));
		}

		if (strcmp(def->defname, "auth_type") == 0)
		{
			if (strcasecmp(defGetString(def), "NOSASL") == 0)
				opt->auth_type = AUTH_TYPE_NOSASL;
			else if (strcasecmp(defGetString(def), "LDAP") == 0)
				opt->auth_type = AUTH_TYPE_LDAP;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\"", defGetString(def)),
						 errhint("Valid auth_type are NOSASL & LDAP.")));
		}

		if (strcmp(def->defname, "log_remote_sql") == 0)
			opt->log_remote_sql = defGetBoolean(def);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			opt->use_remote_estimate = defGetBoolean(def);

		if (strcmp(def->defname, "enable_join_pushdown") == 0)
			opt->enable_join_pushdown = defGetBoolean(def);

		if (strcmp(def->defname, "enable_aggregate_pushdown") == 0)
			opt->enable_aggregate_pushdown = defGetBoolean(def);

		if (strcmp(def->defname, "enable_order_by_pushdown") == 0)
			opt->enable_order_by_pushdown = defGetBoolean(def);

		if (strcmp(def->defname, "fetch_size") == 0)
			opt->fetch_size = atoi(defGetString(def));

		if (strcmp(def->defname, "query_timeout") == 0)
		{
			opt->receive_timeout = atoi(defGetString(def));
			if (opt->receive_timeout <= 0 || opt->receive_timeout >= 100000)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid query timeout \"%s\"",
								defGetString(def)),
						 errhint("Valid range is 1 - 100000 S.")));
			opt->receive_timeout = opt->receive_timeout * 1000;
		}

		if (strcmp(def->defname, "connect_timeout") == 0)
		{
			opt->connect_timeout = atoi(defGetString(def));
			if (opt->connect_timeout <= 0 || opt->connect_timeout >= 100000)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid connect timeout \"%s\"",
								defGetString(def)),
						 errhint("Valid range is 1 - 100000 S.")));
			opt->connect_timeout = opt->connect_timeout * 1000;
		}
	}

	/*
	 * If the table name is not provided, we assume it to be same as foreign
	 * table name.
	 */
	if (!opt->table_name)
		opt->table_name = get_rel_name(foreigntableid);

	return opt;
}
