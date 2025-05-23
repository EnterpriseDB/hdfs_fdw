/*-------------------------------------------------------------------------
 *
 * hdfs_fdw--2.0.4.sql
 * 		Foreign-data wrapper for remote Hadoop servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hdfs_fdw--2.0.4.sql
 *
 *-------------------------------------------------------------------------
 */

/* contrib/hdfs_fdw/hdfs_fdw--2.0.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hdfs_fdw" to load this file. \quit

CREATE FUNCTION hdfs_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION hdfs_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hdfs_fdw
  HANDLER hdfs_fdw_handler
  VALIDATOR hdfs_fdw_validator;

CREATE OR REPLACE FUNCTION hdfs_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;
