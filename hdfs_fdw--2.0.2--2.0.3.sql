/* hdfs_fdw/hdfs_fdw--2.0.2--2.0.3.sql */

CREATE OR REPLACE FUNCTION hdfs_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;
