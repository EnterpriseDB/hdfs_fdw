/*-------------------------------------------------------------------------
 *
 * hdfs_client.c
 * 		Wrapper functions to access APIs from libhive.
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_client.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "hdfs_fdw.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

/*
 * hdfs_fetch
 * 		Gets the next record from the result set.
 */
int
hdfs_fetch(int con_index)
{
	int			rc;
	char	   *err_buf = "unknown";

	rc = DBFetch(con_index, &err_buf);
	if (rc < -1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to fetch data from Hive/Spark server: %s",
						err_buf)));

	return rc;
}

/*
 * hdfs_get_column_count
 * 		Get the number of columns of current result set.
 */
int
hdfs_get_column_count(int con_index)
{
	int			count;
	char	   *err_buf = "unknown";

	count = DBGetColumnCount(con_index, &err_buf);
	if (count < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to get column count from Hive/Spark server: %s",
						err_buf)));

	return count;
}

/*
 * hdfs_get_value
 * 		Convert Hive/Spark Server data into PostgreSQL's compatible data types.
 */
Datum
hdfs_get_value(int con_index, hdfs_opt *opt, Oid pgtyp, int pgtypmod, int idx,
			   bool *is_null)
{
	Datum		value_datum = 0;

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
				regproc		typeinput;
				HeapTuple	tuple;
				int			typemod;
				char	   *value;

				/* Get the type's output functions */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for type %u", pgtyp);

				typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
				typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
				ReleaseSysCache(tuple);

				value = hdfs_get_field_as_cstring(con_index, idx, is_null);

				if (*is_null == true || strlen(value) == 0)
					*is_null = true;
				else
					value_datum = OidFunctionCall3(typeinput,
												   CStringGetDatum(value),
												   ObjectIdGetDatum(pgtyp),
												   Int32GetDatum(typemod));
			}
			break;

		default:
			{
				hdfs_close_result_set(con_index);
				hdfs_rel_connection(con_index);

				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("unsupported PostgreSQL data type"),
						 errhint("Supported data types are BOOL, INT, DATE, TIME, TIMESTAMP, FLOAT, BYTEA, SERIAL, REAL, DOUBLE, CHAR, TEXT, STRING and VARCHAR.")));
			}
			break;
	}

	return value_datum;
}

/*
 * hdfs_get_field_as_cstring
 * 		Retrieves the value of the designated column in the current row of
 * 		active result set object as a cstring.
 */
char *
hdfs_get_field_as_cstring(int con_index, int idx, bool *is_null)
{
	int			size;
	char	   *value;
	char	   *err_buf = "unknown";

	size = DBGetFieldAsCString(con_index, idx, &value, &err_buf);
	if (size < -1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to fetch field from Hive/Spark Server: %s",
						err_buf)));

	if (size == -1)
		*is_null = true;
	else
		*is_null = false;

	return value;
}

/*
 * hdfs_execute_prepared
 * 		Executes a prepared statements.
 */
bool
hdfs_execute_prepared(int con_index)
{
	char	   *err_buf = "unknown";

	if (DBExecutePrepared(con_index, &err_buf) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to execute query: %s", err_buf)));

	return true;
}

/*
 * hdfs_query_execute
 * 		Executes a SELECT query.
 */
bool
hdfs_query_execute(int con_index, hdfs_opt *opt, char *query)
{
	char	   *err_buf = "unknown";

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: execute remote SQL: [%s] [%d]",
			 query, opt->fetch_size);

	if (DBExecute(con_index, query, opt->fetch_size, &err_buf) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to execute query: %s", err_buf)));

	return true;
}

/*
 * hdfs_query_prepare
 * 		Creates a prepared statement to be executed on remote server.
 */
void
hdfs_query_prepare(int con_index, hdfs_opt *opt, char *query)
{
	char	   *err_buf = "unknown";

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: prepare remote SQL: [%s] [%d]",
			 query, opt->fetch_size);

	if (DBPrepare(con_index, query, opt->fetch_size, &err_buf) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to prepare query: %s", err_buf)));
}

/*
 * hdfs_query_execute_utility
 * 		Executes a query that does not return a result set on remote server.
 */
bool
hdfs_query_execute_utility(int con_index, hdfs_opt *opt, char *query)
{
	char	   *err_buf = "unknown";

	if (opt->log_remote_sql)
		elog(LOG, "hdfs_fdw: utility remote SQL: [%s] [%d]",
			 query, opt->fetch_size);

	if (DBExecuteUtility(con_index, query, &err_buf) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to fetch execute query: %s", err_buf)));

	return true;
}

/*
 * hdfs_bind_var
 * 		Bind the prepared query parameters to their respective data types.
 */
bool
hdfs_bind_var(int con_index, int param_index, Oid type, Datum value,
			  bool *isnull)
{
	char	   *err_buf = "unknown";
	int			ret;

	if (*isnull)
	{
		/* TODO: Bind null as a param value yet to handle */
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("cannot bind null parameter"),
				 errhint("Parameter type: %u", type)));
	}

	switch (type)
	{
		case INT2OID:
			{
				int16		dat = DatumGetInt16(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case INT4OID:
			{
				int32		dat = DatumGetInt32(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case INT8OID:
			{
				int64		dat = DatumGetInt64(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case FLOAT4OID:
			{
				float4		dat = DatumGetFloat4(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case FLOAT8OID:
			{
				float8		dat = DatumGetFloat8(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8,
															 value);
				float8		dat = DatumGetFloat8(valueDatum);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case BOOLOID:
			{
				bool		dat = DatumGetBool(value);

				ret = DBBindVar(con_index, param_index, type, &dat, isnull,
								&err_buf);
			}
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case NAMEOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeIsVarLength;

				getTypeOutputInfo(type, &outputFunctionId, &typeIsVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				ret = DBBindVar(con_index, param_index, type, outputString,
								isnull, &err_buf);
			}
			break;
		case DATEOID:
			{
				Datum		valueDatum = DirectFunctionCall1(date_timestamp,
															 value);
				Timestamp	valueTimestamp = DatumGetTimestamp(valueDatum);
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;
				char		buf[50];

				if (timestamp2tm(valueTimestamp, NULL, tm, &fsec,
								 NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							 errmsg("error binding parameter"),
							 errhint("Parameter type: %u", type)));

				snprintf(buf, 50, "%d-%d-%d",
						 tm->tm_year, tm->tm_mon, tm->tm_mday);
				ret = DBBindVar(con_index, param_index, type, buf, isnull,
								&err_buf);
			}
			break;
		case TIMEOID:
			{
				Timestamp	valueTimestamp = DatumGetTimestamp(value);
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;
				char		buf[50];

				if (timestamp2tm(valueTimestamp, NULL, tm, &fsec,
								 NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							 errmsg("error binding parameter"),
							 errhint("Parameter type: %u", type)));

				snprintf(buf, 50, "%d:%d:%d",
						 tm->tm_hour, tm->tm_min, tm->tm_sec);
				ret = DBBindVar(con_index, param_index, type, buf, isnull,
								&err_buf);
			}
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				Timestamp	valueTimestamp = DatumGetTimestamp(value);
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;
				char		buf[100];

				if (timestamp2tm(valueTimestamp, NULL, tm, &fsec,
								 NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							 errmsg("error binding parameter"),
							 errhint("Parameter type: %u", type)));

				snprintf(buf, 100, "%d-%d-%d %d:%d:%d.%d",
						 tm->tm_year, tm->tm_mon, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);
				ret = DBBindVar(con_index, param_index, type, buf, isnull,
								&err_buf);
			}
			break;
		case BITOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeIsVarLength;
				bool		val;

				getTypeOutputInfo(type, &outputFunctionId, &typeIsVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				val = (strcmp(outputString, "0") == 0) ? false : true;

				ret = DBBindVar(con_index, param_index, type, &val, isnull,
								&err_buf);
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot bind parameter"),
					 errhint("Parameter type: %u", type)));
			break;
	}

	if (ret < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to bind variable: %d", type)));

	return true;
}

/*
 * hdfs_close_result_set
 * 		Closes the active result set.
 */
void
hdfs_close_result_set(int con_index)
{
	char	   *err_buf = "unknown";

	DBCloseResultSet(con_index, &err_buf);
}
