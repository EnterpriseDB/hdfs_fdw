/*-------------------------------------------------------------------------
 *
 * hdfs_client.c
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
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
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"

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
				int idx, bool *is_null)
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
								errhint("Supported data types are BOOL, INT, DATE, TIME, TIMESTAMP, FLOAT, BYTEA, SERIAL, REAL, DOUBLE, CHAR, TEXT, STRING and VARCHAR")));
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
hdfs_execute_prepared(int con_index)
{
    char    *err = "unknown";
    char    *err_buf = err;

    if (DBExecutePrepared(con_index, &err_buf) < 0)
        ereport(ERROR,
            (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                errmsg("failed to execute query: %s", err_buf)));
    return true;
}

bool
hdfs_query_execute(int con_index, hdfs_opt *opt, char *query)
{
	char	*err = "unknown";
	char	*err_buf = err;

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: execute remote SQL: [%s] [%d]", query, opt->fetch_size);

	if (DBExecute(con_index, query, opt->fetch_size, &err_buf) < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to execute query: %s", err_buf)));
	return true;
}

bool
hdfs_query_prepare(int con_index, hdfs_opt *opt, char *query)
{
	char	*err = "unknown";
	char	*err_buf = err;

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: prepare remote SQL: [%s] [%d]", query, opt->fetch_size);

	if (DBPrepare(con_index, query, opt->fetch_size, &err_buf) < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to prepare query: %s", err_buf)));
	return true;
}

bool
hdfs_query_execute_utility(int con_index, hdfs_opt *opt, char *query)
{
	char	*err = "unknown";
	char	*err_buf = err;

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: utility remote SQL: [%s] [%d]", query, opt->fetch_size);

	if (DBExecuteUtility(con_index, query, &err_buf) < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch execute query: %s", err_buf)));
	return true;
}

bool
hdfs_bind_var(int con_index, int param_index, Oid type,
					Datum value, bool *isnull)
{
	char	*err = "unknown";
	char	*err_buf = err;
	int		ret;

	if (*isnull)
	{
		/* TODO: Bind null as a param value yet to handle */
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("cannot bind null parameter"),
						errhint("Parameter type: %u", type)));
	}

	switch(type)
	{
		case INT2OID:
		{
			int16 dat = DatumGetInt16(value);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case INT4OID:
		{
			int32 dat = DatumGetInt32(value);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case INT8OID:
		{
			int64 dat = DatumGetInt64(value);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case FLOAT4OID:
		{
			float4 dat = DatumGetFloat4(value);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case FLOAT8OID:
		{
			float8 dat = DatumGetFloat8(value);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case NUMERICOID:
		{
			Datum valueDatum = DirectFunctionCall1(numeric_float8, value);
			float8 dat = DatumGetFloat8(valueDatum);
			ret = DBBindVar(con_index, param_index, type, &dat, isnull, &err_buf);
			break;
		}
		case BOOLOID:
		{
			int32 dat = DatumGetInt32(value);
			bool v;
			if (dat == 0)
				v = false;
			else
				v = true;
			ret = DBBindVar(con_index, param_index, type, &v, isnull, &err_buf);
			break;
		}

		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		{
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);
			ret = DBBindVar(con_index, param_index, type, outputString, isnull, &err_buf);
			break;
		}
		case NAMEOID:
		{
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);
			ret = DBBindVar(con_index, param_index, type, outputString, isnull, &err_buf);
			break;
		}
		case DATEOID:
		{
			Datum			valueDatum = DirectFunctionCall1(date_timestamp, value);
			Timestamp		valueTimestamp = DatumGetTimestamp(valueDatum);
			struct pg_tm	tt, *tm = &tt;
			fsec_t			fsec;
			char			buf[101];

			if (timestamp2tm(valueTimestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("error bindintg parameter"),
						errhint("Parameter type: %u", type)));
			}
			memset(buf, 0, 101);
			snprintf(buf,100, "%d-%d-%d",
			tm->tm_year, tm->tm_mon, tm->tm_mday);
			ret = DBBindVar(con_index, param_index, type, buf, isnull, &err_buf);
			break;
		}
		case TIMEOID:
		{
			Timestamp		valueTimestamp = DatumGetTimestamp(value);
			struct pg_tm	tt, *tm = &tt;
			fsec_t			fsec;
			char			buf[101];

			if (timestamp2tm(valueTimestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("error bindintg parameter"),
						errhint("Parameter type: %u", type)));
			}
			memset(buf, 0, 101);
			snprintf(buf,100, "%d:%d:%d",
			tm->tm_hour, tm->tm_min, tm->tm_sec);
			ret = DBBindVar(con_index, param_index, type, buf, isnull, &err_buf);
			break;
		}

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			Timestamp		valueTimestamp = DatumGetTimestamp(value);
			struct pg_tm	tt, *tm = &tt;
			fsec_t			fsec;
			char			buf[101];

			if (timestamp2tm(valueTimestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("error bindintg parameter"),
						errhint("Parameter type: %u", type)));
			}
			memset(buf, 0, 101);
			snprintf(buf,100, "%d-%d-%d %d:%d:%d.%d",
			tm->tm_year, tm->tm_mon, tm->tm_mday,
			tm->tm_hour, tm->tm_min, tm->tm_sec,fsec);
			ret = DBBindVar(con_index, param_index, type, buf, isnull, &err_buf);
			break;
		}

		case BITOID:
		{
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			bool v;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);

			if (strcmp(outputString, "0") == 0)
				v = false;
			else
				v = true;
			ret = DBBindVar(con_index, param_index, type, &v, isnull, &err_buf);

			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot bind parameter"),
							errhint("Parameter type: %u", type)));
			break;
		}
	}

	if (ret < 0)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to bind variable: %d", type)));
	return true;
}

void
hdfs_close_result_set(int con_index, hdfs_opt *opt)
{
	char	*err = "unknown";
	char	*err_buf = err;

	DBCloseResultSet(con_index, &err_buf);
}
