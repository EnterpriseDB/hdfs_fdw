##Hadoop (HDFS) Foreign Data Wrapper for PostgreSQL (Alpha)

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for [Hadoop][1] (HDFS).

Please note that this version of hdfs_fdw only works with PostgreSQL Version 9.3 and greater.


###What Is Apache [Hadoop][1]?
The Apache™ Hadoop® project develops open-source software for reliable, scalable, distributed computing.

*The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, so delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures. The detail information can be found [here][1]. Hadoop can be downloaded from this [location][2]*.


###What Is Apache [Hive][3].
*The Apache Hive ™ data warehouse software facilitates querying and managing large datasets residing in distributed storage. Hive provides a mechanism to project structure onto this data and query the data using a SQL-like language called HiveQL. At the same time this language also allows traditional map/reduce programmers to plug in their custom mappers and reducers when it is inconvenient or inefficient to express this logic in HiveQL*. There are two version of Hive HiveServer1 and HiveServer2 which can be downloded from this [4][site].


###Installation
To compile the [Hadoop][1] foreign data wrapper, Hive C client library is needed. This library can be downloaded from []Apache][2]

###Download and Install Thrift

- Download Thrift

wget http://www.apache.org/dyn/closer.cgi?path=/thrift/0.9.2/thrift-0.9.2.tar.gz

- Extract Thrift

tar -zxvf thrift-0.9.2.tar.gz

- Compile and install Thrift

```
cd thrift-0.9.2
./configure
make
make install
```

The detail installation manual can be found at http://thrift.apache.org/docs/install/

###Download and Install Hive Client Libraries
```
Hive Client libraries downloaded from these sites
https://svn.apache.org/repos/asf/hive/trunk/service/src/gen/thrift/gen-cpp/
https://svn.apache.org/repos/asf/hive/trunk/odbc/src/cpp
https://svn.apache.org/repos/asf/hive/trunk/ql/src/gen/thrift/gen-cpp/
https://svn.apache.org/repos/asf/hive/trunk/metastore/src/gen/thrift/gen-cpp/
```

###Compile HiveClient library (libhive.so)

```
$ make
```
```
$ make install
```

###Compile and Install hdfs_fdw

1. To build on POSIX-compliant systems you need to ensure the `pg_config` executable is in your path when you run `make`. This executable is typically in your PostgreSQL installation's `bin` directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. Compile the code using make.

    ```
    $ make USE_PGXS=1
    ```

4.  Finally install the foreign data wrapper.

    ```
    # make USE_PGXS=1 install
    ```

Please note that the HDFS_FDW extension has only been tested on ubuntu systems but it should work on other *UNIX's systems without any problems.


##How To Start Hadoop.
The detail installation instruction of hadoop can be found on this [site][5]. Here are the steps to start and stop the hadoop.
 
#### Stop and start Hdfs on Single Node
```
# sbin/stop-dfs.sh
# sbin/start-dfs.sh
```

####YARN on Single Node
```
# sbin/stop-yarn.sh
# sbin/start-yarn.sh
```

##How to Start HiveServer

####Starting HiveServer1
```
cd /usr/local/hive/
bin/hive --service hiveserver1 -v
```

####Starting HiveServer2
```
cd /usr/local/hive/
bin/hive --service hiveserver2 --hiveconf hive.server2.authentication=NOSASL
```

Usage
-----

The following parameters can be set on a HiveServer foreign server object:

  * `host`: Address or hostname of the HiveServer. Defaults to `127.0.0.1`
  * `port`: Port number of the HiveServer. Defaults to `10000`
  * `client_type`:  HiveServer1 or HiveServer2.

The following parameters can be set on a Hive foreign table object:

  * `dbname`: Name of the Hive database to query. This is a mandatory option.
  * `table_name`: Name of the Hive table, default is the same as foreign table.

Step 1: Download [weblogs_parse][8] and follow instructions from this [site][9].

Step 2: Upload weblog_parse.txt file using these commands.

```sh
hadoop fs -mkdir /weblogs
hadoop fs -mkdir /weblogs/parse
hadoop fs -put weblogs_parse.txt /weblogs/parse/part-00000
hadoop fs -cp /weblogs/parse/part-00000 /user/hive/warehouse/weblogs/
```

Step 3: Start HiveServer

```sh
bin/hive --service hiveserver1 -v
```
Step 4: Start Hive client to connect to HiveServer
```sh
hive -h 127.0.0.1
```

Step 5: Create Table in Hive
```sql
CREATE TABLE weblogs (
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
    user_agent          STRING)
row format delimited
fields terminated by '\t';
```

Now we are ready to use the the weblog table in PostgreSQL, we need to follow these steps.

```sql
-- load extension first time after install
CREATE EXTENSION hdfs_fdw;

-- create server object
CREATE SERVER hdfs_server
         FOREIGN DATA WRAPPER hdfs_fdw
         OPTIONS (host '127.0.0.1');

-- create user mapping
CREATE USER MAPPING FOR postgres
    SERVER hdfs_server;

-- create foreign table
CREATE FOREIGN TABLE weblogs
(
 client_ip                TEXT,
 full_request_date        TEXT,
 day                      TEXT,
 Month                    TEXT,
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
         OPTIONS (dbname 'db', table_name 'weblogs');


-- select from table
postgres=# SELECT DISTINCT client_ip IP, count(*)
           FROM weblogs GROUP BY IP HAVING count(*) > 5000;
       ip        | count
-----------------+-------
 683.615.622.618 | 13505
 14.323.74.653   | 16194
 13.53.52.13     |  5494
 361.631.17.30   | 64979
 363.652.18.65   | 10561
 325.87.75.36    |  6498
 322.6.648.325   | 13242
 325.87.75.336   |  6500
(8 rows)


CREATE TABLE premium_ip
(
      client_ip TEXT, category TEXT
);

INSERT INTO premium_ip VALUES ('683.615.622.618','Category A');
INSERT INTO premium_ip VALUES ('14.323.74.653','Category A');
INSERT INTO premium_ip VALUES ('13.53.52.13','Category A');
INSERT INTO premium_ip VALUES ('361.631.17.30','Category A');
INSERT INTO premium_ip VALUES ('361.631.17.30','Category A');
INSERT INTO premium_ip VALUES ('325.87.75.336','Category B');

postgres=# SELECT hd.client_ip IP, pr.category, count(hd.client_ip)
                           FROM weblogs hd, premium_ip pr
                           WHERE hd.client_ip = pr.client_ip
                           AND hd.year = '2011'                                                
                           GROUP BY hd.client_ip,pr.category;
                           
       ip        |  category  | count
-----------------+------------+-------
 14.323.74.653   | Category A |  9459
 361.631.17.30   | Category A | 76386
 683.615.622.618 | Category A | 11463
 13.53.52.13     | Category A |  3288
 325.87.75.336   | Category B |  3816
(5 rows)


postgres=# EXPLAIN VERBOSE SELECT hd.client_ip IP, pr.category, count(hd.client_ip)
                           FROM weblogs hd, premium_ip pr
                           WHERE hd.client_ip = pr.client_ip
                           AND hd.year = '2011'                                                
                           GROUP BY hd.client_ip,pr.category;
                                          QUERY PLAN                                          
----------------------------------------------------------------------------------------------
 HashAggregate  (cost=221.40..264.40 rows=4300 width=64)
   Output: hd.client_ip, pr.category, count(hd.client_ip)
   Group Key: hd.client_ip, pr.category
   ->  Merge Join  (cost=120.35..189.15 rows=4300 width=64)
         Output: hd.client_ip, pr.category
         Merge Cond: (pr.client_ip = hd.client_ip)
         ->  Sort  (cost=60.52..62.67 rows=860 width=64)
               Output: pr.category, pr.client_ip
               Sort Key: pr.client_ip
               ->  Seq Scan on public.premium_ip pr  (cost=0.00..18.60 rows=860 width=64)
                     Output: pr.category, pr.client_ip
         ->  Sort  (cost=59.83..62.33 rows=1000 width=32)
               Output: hd.client_ip
               Sort Key: hd.client_ip
               ->  Foreign Scan on public.weblogs hd  (cost=100.00..10.00 rows=1000 width=32)
                     Output: hd.client_ip
                     Remote SQL: SELECT client_ip FROM weblogs WHERE ((year = '2011'))
```

##TODO
1. Hadoop Installation Instructions
2. Write-able support
3. Flum support
4. Regression test cases

##Contributing
If you experince any bug create new [issue][2] and if you have fix for that create a pull request.
Before submitting a bugfix or new feature, please read the [contributing guidlines][6].

##Support
This project will be modified to maintain compatibility with new PostgreSQL releases.

As with many open source projects, you may be able to obtain support via the public mailing list (hdfs_fdw @ enterprisedb.com).
If you require commercial support, please contact the EnterpriseDB sales team, or check whether your existing PostgreSQL support provider can also support hdfs_fdw.


###License
Copyright (c) 2011 - 2014, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph and
the following two paragraphs appear in all copies.

See the [`LICENSE`][10] file for full details.

[1]: http://www.appache.com
[2]: http://hadoop.apache.org/releases.html
[3]: https://hive.apache.org/
[4]: https://hive.apache.org/downloads.html
[5]: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html
[6]: https://github.com/EnterpriseDB/hdfs_fdw/issues/new
[7]: CONTRIBUTING.md
[8]: http://wiki.pentaho.com/download/attachments/23531451/weblogs_parse.zip?version=1&modificationDate=1327096242000
[9]: http://wiki.pentaho.com/display/BAD/Transforming+Data+within+Hive
[10]: LICENSE