Hadoop (HDFS) Foreign Data Wrapper for PostgreSQL
-------------------------------------------------
This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for 
[Hadoop][1] (HDFS).

Please note that this version of hdfs_fdw works with PostgreSQL and EDB 
Postgres Advanced Server 9.5, 9.6, 10, 11, 12, and 13.

Installation
------------
See the file [`INSTALL`][2] for instructions on how to build and install
the extension and it's dependencies.

What Is Apache [Hadoop][1]?
--------------
The Apache™ Hadoop® project develops open-source software for
reliable, scalable, distributed computing. The Apache Hadoop software
library is a framework that allows for the distributed processing of
large data sets across clusters of computers using simple programming
models. It is designed to scale up from single servers to thousands of
machines, each offering local computation and storage. Rather than rely
on hardware to deliver high-availability, the library itself is designed
to detect and handle failures at the application layer, so delivering a
highly-available service on top of a cluster of computers, each of which
may be prone to failures. The detail information can be found [here][1].
Hadoop can be downloaded from this [location][3], and can be installed
by following the steps given [here][4].


What Is Apache [Hive][5]?
--------------
The Apache Hive ™ data warehouse software facilitates querying and
managing large datasets residing in distributed storage. Hive provides
a mechanism to project structure onto this data and query the data
using a SQL-like language called HiveQL. At the same time this language
also allows traditional map/reduce programmers to plug in their custom
mappers and reducers when it is inconvenient or inefficient to express
this logic in HiveQL.

There are two version of Hive HiveServer1 and HiveServer2 which can be
downloaded from this [site][6]. The FDW supports only HiveServer2.


What Is Apache [Spark][7]?
--------------
The Apache Spark ™ is a general purpose distributed computing
framework which supports a wide variety of use cases. It provides real
time stream as well as batch processing with speed, ease of use and
sophisticated analytics. Spark does not provide storage layer, it relies
on third party storage providers like Hadoop, HBASE, Cassandra, S3
etc. Spark integrates seamlessly with Hadoop and can process existing
data. Spark SQL is 100% compatible with HiveQL and can be used as a
replacement of HiveServer2, using Spark Thrift Server.


Authentication Support
-----

The FDW supports NOSASL and LDAP authentication modes. In order to use
NOSASL do not specify any OPTIONS while creating user mapping. For LDAP
username and password must be specified in OPTIONS while creating user
mapping.


Usage
-----

While creating the foreign server object for HDFS FDW the following can
be specified in options:

  * `host`: IP Address or hostname of the Hive Thrift Server OR Spark
	Thrift Server. Defaults to `localhost`.
  * `port`: Port number of the Hive Thrift Server OR Spark Thrift
	Server. Defaults to `10000`.
  * `client_type`:  hiveserver2 or spark. Hive and Spark both support
	HiveQL and are compatible but there are few differences like the
	behaviour of ANALYZE command and connection string for the NOSASL case.
	Default is `hiveserver2`.
  * `auth_type`: NOSASL or LDAP. Specify which authentication type
	is required while connecting to the Hive or Spark server. Default is
	unspecified and the FDW uses the username option in the user mapping to
	infer the auth_type. If the username is empty or not specified it uses
	NOSASL.
  * `connect_timeout`:  Connection timeout, default value is `300` seconds.
  * `query_timeout`:  Query timeout is not supported by the Hive JDBC
	driver.
  * `fetch_size`:  A user-specified value that is provided as a parameter
	to the JDBC API setFetchSize. The default value is `10000`.
  * `log_remote_sql`:  If true, logging will include SQL commands
	executed on the remote hive server and the number of times that a scan
	is repeated. The default is false.
  * `use_remote_estimate`: Include the use_remote_estimate to instruct
	the server to use EXPLAIN commands on the remote server when estimating
	processing costs. By default, use_remote_estimate is false, and remote
	tables are assumed to have `1000` rows.

When creating user mapping following options can be provided:

  * `username`: The name of the user for authentication on the Hive server.
  * `password`: The password of the user for authentication on the Hive
	server.

HDFS can be used through either Hive or Spark. In this case both Hive
and Spark store metadata in the configured metastore. In the metastore
databases and tables can be created using HiveQL. While creating foreign
table object for the foreign server the following can be specified in
options:

  * `dbname`: Name of the metastore database to query. Default is
	`'default'`.
  * `table_name`: Name of the metastore table. Default is the same as
	foreign table name.


Using HDFS FDW with Apache Hive on top of Hadoop
-----

Step 1: Download [weblogs_parse][8] and follow instructions from this
  [site][9].

Step 2: Upload `weblog_parse.txt` file using these commands:

```sh
hadoop fs -mkdir /weblogs
hadoop fs -mkdir /weblogs/parse
hadoop fs -put weblogs_parse.txt /weblogs/parse/part-00000
```

Step 3: Start HiveServer if not already running using following command:

```sh
$HIVE_HOME/bin/hiveserver2
```

or

```sh
$HIVE_HOME/bin/hive --service hiveserver2
```

Step 4: Connect to HiveServer2 using hive beeline client.

e.g.
```sh
$ beeline
Beeline version 1.0.1 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000/default;auth=noSasl
```

Step 5: Create Table in Hive
```sql
CREATE TABLE weblogs
	(
		client_ip           STRING,
		full_request_date   STRING,
		day                 STRING,
		month               STRING,
		month_num           INT,
		year                STRING,
		hour                STRING,
		minute              STRING,
		second              STRING,
		timezone            STRING,
		http_verb           STRING,
		uri                 STRING,
		http_status_code    STRING,
		bytes_returned      STRING,
		referrer            STRING,
		user_agent          STRING
	)
	row format delimited
	fields terminated by '\t';
```

Step 6: Load data in weblogs table:
```sh
hadoop fs -cp /weblogs/parse/part-00000 /user/hive/warehouse/weblogs/
```

Step 7: Access data from PostgreSQL:

Now we are ready to use the the weblog table in PostgreSQL, we need to
follow these steps once we are connected using psql:

```sql
-- set the GUC variables appropriately, e.g. :
hdfs_fdw.jvmpath='/home/edb/Projects/hadoop_fdw/jdk1.8.0_111/jre/lib/amd64/server/'
hdfs_fdw.classpath='/usr/local/edbas/lib/postgresql/HiveJdbcClient-1.0.jar:
                    /home/edb/Projects/hadoop_fdw/hadoop/share/hadoop/common/hadoop-common-2.6.4.jar:
                    /home/edb/Projects/hadoop_fdw/apache-hive-1.0.1-bin/lib/hive-jdbc-1.0.1-standalone.jar'

-- load extension first time after install
CREATE EXTENSION hdfs_fdw;

-- create server object
CREATE SERVER hdfs_server
	FOREIGN DATA WRAPPER hdfs_fdw
	OPTIONS (host '127.0.0.1');

-- create user mapping
CREATE USER MAPPING FOR postgres
	SERVER hdfs_server OPTIONS (username 'hive_username', password 'hive_password');

-- create foreign table
CREATE FOREIGN TABLE weblogs
	(
		client_ip                TEXT,
		full_request_date        TEXT,
		day                      TEXT,
		month                    TEXT,
		month_num                INTEGER,
		year                     TEXT,
		hour                     TEXT,
		minute                   TEXT,
		second                   TEXT,
		timezone                 TEXT,
		http_verb                TEXT,
		uri                      TEXT,
		http_status_code         TEXT,
		bytes_returned           TEXT,
		referrer                 TEXT,
		user_agent               TEXT
	)
	SERVER hdfs_server
	OPTIONS (dbname 'default', table_name 'weblogs');

-- select from table
SELECT DISTINCT client_ip IP, count(*)
	FROM weblogs GROUP BY IP HAVING count(*) > 5000 ORDER BY 1;
       ip        | count 
-----------------+-------
 13.53.52.13     |  5494
 14.323.74.653   | 16194
 322.6.648.325   | 13242
 325.87.75.336   |  6500
 325.87.75.36    |  6498
 361.631.17.30   | 64979
 363.652.18.65   | 10561
 683.615.622.618 | 13505
(8 rows)

-- EXPLAIN output showing WHERE clause being pushed down to remote server.
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT client_ip, full_request_date, uri FROM weblogs
	WHERE http_status_code = 200;
                                                   QUERY PLAN                                                   
----------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.weblogs
   Output: client_ip, full_request_date, uri
   Remote SQL: SELECT client_ip, full_request_date, uri FROM default.weblogs WHERE ((http_status_code = '200'))
(3 rows)
```

Using HDFS FDW with Apache Spark on top of Hadoop
-----

Step 1: Download & install Apache Spark in local mode.

Step 2: In the folder ``$SPARK_HOME/conf`` create a file
``spark-defaults.conf`` containing the following line
```sql
spark.sql.warehouse.dir hdfs://localhost:9000/user/hive/warehouse
```

By default spark uses derby for both meta data and the data itself
(called warehouse in spark). In order to have spark use hadoop as
warehouse we have to add this property.

Step 3: Start Spark Thrift Server
```sql
./start-thriftserver.sh
```

Step 4: Make sure Spark thrift server is running using log file

Step 5: Create a local file names.txt with below data:
```sh
$ cat /tmp/names.txt
1,abcd
2,pqrs
3,wxyz
4,a_b_c
5,p_q_r
,
```

Step 6: Connect to Spark Thrift Server2 using spark beeline client.

e.g.
```sh
$ beeline
Beeline version 1.2.1.spark2 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000/default;auth=noSasl org.apache.hive.jdbc.HiveDriver
```

Step 7: Getting the sample data ready on spark:

Run the following commands in beeline command line tool:-

```sql
./beeline
Beeline version 1.2.1.spark2 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000/default;auth=noSasl org.apache.hive.jdbc.HiveDriver
Connecting to jdbc:hive2://localhost:10000/default;auth=noSasl
Enter password for jdbc:hive2://localhost:10000/default;auth=noSasl: 
Connected to: Spark SQL (version 2.1.1)
Driver: Hive JDBC (version 1.2.1.spark2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://localhost:10000> create database my_test_db;
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.379 seconds)
0: jdbc:hive2://localhost:10000> use my_test_db;
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.03 seconds)
0: jdbc:hive2://localhost:10000> create table my_names_tab(a int, name string)
                                 row format delimited fields terminated by ' ';
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.11 seconds)
0: jdbc:hive2://localhost:10000>

0: jdbc:hive2://localhost:10000> load data local inpath '/tmp/names.txt'
                                 into table my_names_tab;
+---------+--+
| Result  |
+---------+--+
+---------+--+
No rows selected (0.33 seconds)
0: jdbc:hive2://localhost:10000> select * from my_names_tab;
+-------+---------+--+
|   a   |  name   |
+-------+---------+--+
| 1     | abcd    |
| 2     | pqrs    |
| 3     | wxyz    |
| 4     | a_b_c   |
| 5     | p_q_r   |
| NULL  | NULL    |
+-------+---------+--+
```

Here are the corresponding files in hadoop:

```sql
$ hadoop fs -ls /user/hive/warehouse/
Found 1 items
drwxrwxrwx   - org.apache.hive.jdbc.HiveDriver supergroup          0 2020-06-12 17:03 /user/hive/warehouse/my_test_db.db

$ hadoop fs -ls /user/hive/warehouse/my_test_db.db/
Found 1 items
drwxrwxrwx   - org.apache.hive.jdbc.HiveDriver supergroup          0 2020-06-12 17:03 /user/hive/warehouse/my_test_db.db/my_names_tab
```

Step 8: Access data from PostgreSQL:

Connect to Postgres using psql:
```sql
-- set the GUC variables appropriately, e.g. :
hdfs_fdw.jvmpath='/home/edb/Projects/hadoop_fdw/jdk1.8.0_111/jre/lib/amd64/server/'
hdfs_fdw.classpath='/usr/local/edbas/lib/postgresql/HiveJdbcClient-1.0.jar:
                    /home/edb/Projects/hadoop_fdw/hadoop/share/hadoop/common/hadoop-common-2.6.4.jar:
                    /home/edb/Projects/hadoop_fdw/apache-hive-1.0.1-bin/lib/hive-jdbc-1.0.1-standalone.jar'

-- load extension first time after install
CREATE EXTENSION hdfs_fdw;

-- create server object
CREATE SERVER hdfs_server
	FOREIGN DATA WRAPPER hdfs_fdw
	OPTIONS (host '127.0.0.1', port '10000', client_type 'spark', auth_type 'NOSASL');

-- create user mapping
CREATE USER MAPPING FOR postgres
	SERVER hdfs_server OPTIONS (username 'spark_username', password 'spark_password');

-- create foreign table
CREATE FOREIGN TABLE f_names_tab( a int, name varchar(255)) SERVER hdfs_svr
	OPTIONS (dbname 'testdb', table_name 'my_names_tab');

-- select the data from foreign server
SELECT * FROM f_names_tab;
 a |  name 
---+--------
 1 | abcd
 2 | pqrs
 3 | wxyz
 4 | a_b_c
 5 | p_q_r
 0 |
(6 rows)

-- EXPLAIN output showing WHERE clause being pushed down to remote server.
EXPLAIN (verbose, costs off)
	SELECT name FROM f_names_tab
	WHERE a > 3;
                                QUERY PLAN                                
--------------------------------------------------------------------------
 Foreign Scan on public.f_names_tab
   Output: name
   Remote SQL: SELECT name FROM my_test_db.my_names_tab WHERE ((a > '3'))
(3 rows)
```

Please note that we are using the same port while creating foreign
server because Spark Thrift Server is compatible with Hive Thrift
Server. Applications using Hiveserver2 would work with Spark except
for the behaviour of ANALYZE command and the connection string in case
of NOSASL. It is better to use ALTER SERVER and change the client_type
option if Hive is to be replaced with Spark.


Contributing
------------
If you experience any bug create new [issue][10] and if you have fix for
that create a pull request. Before submitting a bug-fix or new feature,
please read the [contributing guidelines][11].


Support
-------
This project will be modified to maintain compatibility with new
PostgreSQL and EDB Postgres Advanced Server releases.

If you require commercial support, please contact the EnterpriseDB sales
team, or check whether your existing PostgreSQL support provider can
also support hdfs_fdw.


Copyright Information
---------------------
Copyright (c) 2011 - 2020, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice
and this paragraph and the following two paragraphs appear in all
copies.

IN NO EVENT SHALL ENTERPRISEDB CORPORATION BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND
ITS DOCUMENTATION, EVEN IF ENTERPRISEDB CORPORATION HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

ENTERPRISEDB CORPORATION SPECIFICALLY DISCLAIMS ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE
PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND ENTERPRISEDB
CORPORATION HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT,
UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

See the [`LICENSE`][12] file for full details.

[1]: https://hadoop.apache.org
[2]: INSTALL
[3]: http://hadoop.apache.org/releases.html
[4]: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
[5]: https://hive.apache.org/
[6]: https://hive.apache.org/downloads.html
[7]: http://spark.apache.org/
[8]: http://wiki.pentaho.com/download/attachments/23531451/weblogs_parse.zip?version=1&modificationDate=1327096242000
[9]: http://wiki.pentaho.com/display/BAD/Transforming+Data+within+Hive
[10]: https://github.com/EnterpriseDB/hdfs_fdw/issues/new
[11]: CONTRIBUTING.md
[12]: LICENSE
