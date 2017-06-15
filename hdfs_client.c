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
#include "utils/lsyscache.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"

#include "libhive/jdbc/hiveclient.h"

#include "hdfs_fdw.h"

int
hdfs_fetch(int con_index, hdfs_opt *opt)
{
	int		rc;
	char	*err = "unknown";
	char	*err_buf = err;

	rc = DBFetch(con_index, &err_buf);
	if (rc < -1)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch data from HiveServer: %s", err_buf)));
	return rc;
}

int
hdfs_get_column_count(int con_index, hdfs_opt *opt)
{
	int		count;
	char	*err = "unknown";
	char	*err_buf = err;

	count = DBGetColumnCount(con_index, &err_buf);
	if (count < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to get column count HiveServer: %s", err_buf)));
	return count;
}

Datum
hdfs_get_value(int con_index, hdfs_opt *opt, Oid pgtyp, int pgtypmod,
				int idx, bool *is_null, int col_type)
{
	Datum      value_datum = 0;
	regproc    typeinput;
	HeapTuple  tuple;
	int        typemod;
	char       *value;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", pgtyp);

	typeinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
	typemod  = ((Form_pg_type)GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	switch (pgtyp)
	{
		case BITOID:
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case BYTEAOID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case FLOAT4OID:
 		case FLOAT8OID:
		case CHAROID:
		case NAMEOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			value = hdfs_get_field_as_cstring(con_index, opt, idx, is_null);
			if (*is_null == true || strlen(value) == 0)
			{
				*is_null = true;
			}
			else
			{
				value_datum = OidFunctionCall3(typeinput, CStringGetDatum(value),
												ObjectIdGetDatum(pgtyp),
												Int32GetDatum(typemod));
			}
		}
		break;
		default:
		{
			hdfs_close_result_set(con_index, opt);
			hdfs_rel_connection(con_index);

			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("unsupported PostgreSQL data type"),
								errhint("Supported data types are BOOL, INT, DATE, TIME, TIMESTAMP, FLOAT, BYTEA, SERIAL, REAL, DOUBLE, CHAR, TEXT, STRING and VARCHAR : %u", pgtyp)));
                        break;
		}
	}
	return value_datum;
}

char *
hdfs_get_field_as_cstring(int con_index, hdfs_opt *opt, int idx, bool *is_null)
{
	int		size;
	char    *value;
	int     isnull = 1;
	char	*err = "unknown";
	char	*err_buf = err;

	size = DBGetFieldAsCString(con_index, idx, &value, &err_buf);
	if (size < -1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("failed to fetch field from HiveServer: %s", err_buf)));

	if (size == -1)
	{
		*is_null = true;
	}
	else
	{
		*is_null = false;
	}

	return value;
}

bool
hdfs_query_execute(int con_index, hdfs_opt *opt, char *query)
{
	char	*err = "unknown";
	char	*err_buf = err;

	if (DBExecute(con_index, query, 1000, &err_buf) < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch execute query: %s", err_buf)));
	return true;
}

bool
hdfs_query_execute_utility(int con_index, hdfs_opt *opt, char *query)
{
	char	*err = "unknown";
	char	*err_buf = err;

	if (DBExecuteUtility(con_index, query, &err_buf) < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch execute query: %s", err_buf)));
	return true;
}

void
hdfs_close_result_set(int con_index, hdfs_opt *opt)
{
	char	*err = "unknown";
	char	*err_buf = err;

	DBCloseResultSet(con_index, &err_buf);
}
