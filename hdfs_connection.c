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
 * 		connection.c
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

/* Length of host */
#define HOST_LEN 256

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID plus the user
 * mapping OID.  (We use just one connection per user per foreign server,
 * so that we can ensure all scans use the same snapshot during a query.)
 */
typedef struct ConnCacheKey
{
	Oid			serverid;		/* OID of foreign server */
	Oid			userid;			/* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey    key;             /* hash key (must be first) */
	HiveConnection *conn;            /* connection to foreign server, or NULL */
	CLIENT_TYPE    client_type;      /* HIVESERVER1 or HIVESERVER2 */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

static HiveConnection *hdfs_connect(hdfs_opt *opt);
static HiveReturn hdfs_disconnect(CLIENT_TYPE client_type, HiveConnection *conn);

/*
 * hdfs_get_connection:
 * 			Get a connection which can be used to execute queries on
 * the remote HDFS server with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
HiveConnection*
hdfs_get_connection(ForeignServer *server, UserMapping *user, hdfs_opt *opt)
{
	bool found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL	ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;

		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("hdfs_fdw connections", 8,
									&ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key.serverid = server->serverid;
	key.userid = user->userid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */
		entry->conn = NULL;
	}
	if (entry->conn == NULL)
	{
		entry->conn = hdfs_connect(opt);
		elog(DEBUG3, "new hdfs_fdw connection %p for server \"%s\"",
			 entry->conn, server->servername);
	}
	return entry->conn;
}

/*
 * cleanup_connection:
 * Delete all the cache entries on backend exists.
 */
void
hdfs_cleanup_connection(void)
{
	HASH_SEQ_STATUS	scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		elog(DEBUG3, "disconnecting hdfs_fdw connection %p", entry->conn);
		hdfs_disconnect(entry->client_type, entry->conn);
		entry->conn = NULL;
	}
}

/*
 * Release connection created by calling GetConnection.
 */
void
hdfs_rel_connection(hdfs_opt *opt)
{
	/*
	 * We don't close the connection indvisually  here, will do all connection
	 * cleanup on the backend exit.
	 */
}

static HiveConnection*
hdfs_connect(hdfs_opt *opt)
{
	HiveConnection *conn = NULL;
	char           err_buf[512];
	conn = DBOpenConnection(opt->dbname,
				opt->host,
				opt->port,
				0,
				opt->client_type,
				err_buf,
				512);
	if (!conn)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				errmsg("failed to initialize the HDFS connection object (%s)", err_buf)));
	return conn;
}

static HiveReturn
hdfs_disconnect(CLIENT_TYPE client_type, HiveConnection *conn)
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
	return r;
}
