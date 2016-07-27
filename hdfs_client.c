/*-------------------------------------------------------------------------
 *
 * hdfs_client.c
 * 		Foreign-data wrapper for remote Hadoop servers
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

size_t
hdfs_get_column_count(hdfs_opt *opt, HiveResultSet *rs)
{
	size_t count = 0;
	char err_buf[512];
	if (DBGetColumnCount(rs, &count, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to get column count HiveServer: %s", err_buf)));
	return count;
}

size_t
hdfs_get_field_data_len(hdfs_opt *opt, HiveResultSet *rs, int col)
{
	size_t len;
	char err_buf[512];
	if (DBGetFieldDataLen(rs, col, &len, err_buf, sizeof(err_buf)) == HIVE_ERROR)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to fetch field length from HiveServer: %s", err_buf)));
	return len;
}

Datum
hdfs_get_value(hdfs_opt *opt, Oid pgtyp, int pgtypmod, HiveResultSet *rs, int idx, bool *is_null, int len, int col_type)
{
	Datum      value_datum = 0;
	Datum      valueDatum = 0;
	regproc    typeinput;
	HeapTuple  tuple;
	int        typemod;
	char *value;
	char str[10];

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
 		{
			char *value;
			value = hdfs_get_field_as_cstring(opt, rs, idx, is_null, len);
			switch (col_type)
			{
				case HDFS_TINYINT:
				{
					if (strlen(value) == 0)
					{
						*is_null = true;
					}
					else
					{
						sprintf(str, "%d", value[0]);
						valueDatum = CStringGetDatum((char*)str);
						value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(pgtyp), Int32GetDatum(typemod));
					}
				}
				break;
				default:
				{
					/* libhive return an empty string for null value */
					if (strlen(value) == 0)
					{
						*is_null = true;
					}
					else
					{
						valueDatum = CStringGetDatum((char*)value);
						value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(pgtyp), Int32GetDatum(typemod));
					}
				}
				break;
			}
		}
		break;
		case CHAROID:
		case NAMEOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			value = hdfs_get_field_as_cstring(opt, rs, idx, is_null, len);
			switch (col_type)
			{
				case HDFS_TINYINT:
				{
					if (strlen(value) == 0)
					{
						*is_null = true;
					}
					else
					{
						sprintf(str, "%d", value[0]);
						valueDatum = CStringGetDatum((char*)str);
						value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(pgtyp), Int32GetDatum(typemod));
					}
				}
				break;
				default:
				{
					valueDatum = CStringGetDatum((char*)value);
					value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(pgtyp), Int32GetDatum(typemod));
				}
				break;
			}
		}
		break;
		default:
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("unsupported PostgreSQL data type"),
								errhint("Supported data types are BOOL, INT, DATE, TIME, TIMESTAMP, FLOAT, BYTEA, SERIAL, REAL, DOUBLE, CHAR, TEXT, STRING and VARCHAR : %u", pgtyp)));
                        break;

	}
	return value_datum;
}


char *
hdfs_get_field_as_cstring(hdfs_opt *opt, HiveResultSet *rs, int idx, bool *is_null, int len)
{
	size_t  bs;
	char    *value = NULL;
	int     isnull = 1;
	char    err_buf[512];

	value = (char*) palloc(len);
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
hdfs_query_execute(HiveConnection *conn, hdfs_opt *opt, char *query)
{
	HiveResultSet *rs = NULL;
	char  err_buf[512];

	if (DBExecute(conn, query, &rs, 1000, err_buf, sizeof(err_buf)) != HIVE_SUCCESS)
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
