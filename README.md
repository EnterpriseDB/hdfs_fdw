Hadoop (HDFS) Foreign Data Wrapper for PostgreSQL
-------------------------------------------------
This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for 
[Hadoop][1] (HDFS).

Please note that this version of hdfs_fdw works with PostgreSQL and EDB 
Postgres Advanced Server 9.3, 9.4, 9.5 and 9.6.

What Is Apache [Hadoop][1]?
--------------
*The Apache™ Hadoop® project develops open-source software for reliable, 
scalable, distributed computing.The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, so delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures. The detail information can be found [here][1]. Hadoop 
can be downloaded from this [location][2]*.
  
  
What Is Apache [Hive][3]?
--------------
*The Apache Hive ™ data warehouse software facilitates querying and managing 
large datasets residing in distributed storage. Hive provides a mechanism to 
project structure onto this data and query the data using a SQL-like language 
called HiveQL. At the same time this language also allows traditional 
map/reduce programmers to plug in their custom mappers and reducers when it is
inconvenient or inefficient to express this logic in HiveQL*. 

There are two version of Hive HiveServer1 and HiveServer2 which can be downloaded from this [site][4].
The FDW supports only HiveServer2.

  
What Is Apache [Spark][11]?
--------------
The Apache Spark ™ is a general purpose distributed computing framework which supports a wide variety of uses cases. It provides real time stream as well as batch processing with speed, ease of use and sophisticated analytics. Spark does not provide storage layer, it relies on third party storage providers like Hadoop, HBASE, Cassandra, S3 etc. Spark integrates seamlessly with Hadoop and can process existing data. Spark SQL is 100% compatible with HiveQL and can be used as a replacement of hiveserver2, using Spark Thrift Server.

  
Authentication Support
-----
  
The FDW supports NOSASL and LDAP authentication modes.
In order to use NOSASL do not specify any OPTIONS while creating user mapping.
For LDAP username and password must be specified in OPTIONS while creating user mapping.

  
Usage
-----
  
While creating the foreign server object for HDFS FDW the following can be specified in options:
  
    * `host`: IP Address or hostname of the Hive Thrift Server OR Spark Thrift Server. Defaults to `127.0.0.1`
    * `port`: Port number of the Hive Thrift Server OR Spark Thrift Server. Defaults to `10000`
    * `client_type`:  HiveServer2. HiveServer1 is not supported. This option will be deprecated soon.
    * `connect_timeout`:  Connection timeout, default value is 300 seconds.
    * `query_timeout`:  Query timeout is not supported by the Hive JDBC driver.
  
HDFS can be used through either Hive or Spark. In this case both Hive and Spark store metadata in the configured metastore. In the metastore databases and tables can be created using HiveQL. While creating foreign table object for the foreign server the following can be specified in options:
  
    * `dbname`: Name of the metastore database to query. This is a mandatory option.
    * `table_name`: Name of the metastore table, default is the same as foreign table name.

  
Using HDFS FDW with Apache Hive on top of Hadoop
-----
    
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
  bin/hive --service hiveserver -v
  ```
Step 4: Start beeline client to connect to HiveServer
  ```sh
  ./beeline
  Beeline version 1.0.1 by Apache Hive
  beeline> !connect jdbc:hive2://localhost:10000 'ldapadm' 'abcdef'  org.apache.hive.jdbc.HiveDriver
  Connecting to jdbc:hive2://localhost:10000
  Connected to: Spark SQL (version 2.1.0)
  Driver: Hive JDBC (version 1.0.1)
  Transaction isolation: TRANSACTION_REPEATABLE_READ
  0: jdbc:hive2://localhost:10000>
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
  
  Now we are ready to use the the weblog table in PostgreSQL, we need to follow 
  these steps.
  
  ```sql
  -- export LD_LIBRARY_PATH before starting the server, for example
  export LD_LIBRARY_PATH=/home/edb/Projects/hadoop_fdw/jdk1.8.0_111/jre/lib/amd64/server/:/usr/local/edb95/lib/postgresql/
  
  -- set the GUC class path variable
  hdfs_fdw.classpath='/usr/local/edb95/lib/postgresql/HiveJdbcClient-1.0.jar:
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
      SERVER hdfs_server OPTIONS (username 'ldapadm', password 'abcdef');
  
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
  
  
  postgres=# EXPLAIN VERBOSE SELECT hd.client_ip IP, pr.category, 
  count(hd.client_ip)
                             FROM weblogs hd, premium_ip pr
                             WHERE hd.client_ip = pr.client_ip
                             AND hd.year = '2011'                                 
                 
                             GROUP BY hd.client_ip,pr.category;
                                            QUERY PLAN                            
                
  --------------------------------------------------------------------------------
  --------------
   HashAggregate  (cost=221.40..264.40 rows=4300 width=64)
     Output: hd.client_ip, pr.category, count(hd.client_ip)
     Group Key: hd.client_ip, pr.category
     ->  Merge Join  (cost=120.35..189.15 rows=4300 width=64)
           Output: hd.client_ip, pr.category
           Merge Cond: (pr.client_ip = hd.client_ip)
           ->  Sort  (cost=60.52..62.67 rows=860 width=64)
                 Output: pr.category, pr.client_ip
                 Sort Key: pr.client_ip
                 ->  Seq Scan on public.premium_ip pr  (cost=0.00..18.60 rows=860 
  width=64)
                       Output: pr.category, pr.client_ip
           ->  Sort  (cost=59.83..62.33 rows=1000 width=32)
                 Output: hd.client_ip
                 Sort Key: hd.client_ip
                 ->  Foreign Scan on public.weblogs hd  (cost=100.00..10.00 
  rows=1000 width=32)
                       Output: hd.client_ip
                       Remote SQL: SELECT client_ip FROM weblogs WHERE ((year = 
  '2011'))
  ```

Using HDFS FDW with Apache Spark on top of Hadoop
-----


1. Install PPAS 9.5 and hdfs_fdw using installer.

2. Export LD_LIBRARY_PATH before starting the server, for example
  export LD_LIBRARY_PATH=/home/edb/Projects/hadoop_fdw/jdk1.8.0_111/jre/lib/amd64/server/:/usr/local/edb95/lib/postgresql/
  
3. Set the GUC class path variable
  hdfs_fdw.classpath='/usr/local/edb95/lib/postgresql/HiveJdbcClient-1.0.jar:
                      /home/edb/Projects/hadoop_fdw/hadoop/share/hadoop/common/hadoop-common-2.6.4.jar:
                      /home/edb/Projects/hadoop_fdw/apache-hive-1.0.1-bin/lib/hive-jdbc-1.0.1-standalone.jar'
  
4. At the edb-psql prompt issue the following commands.
    ```sql
        CREATE EXTENSION hdfs_fdw;
        CREATE SERVER hdfs_svr FOREIGN DATA WRAPPER hdfs_fdw
        OPTIONS (host '127.0.0.1',port '10000',client_type 'hiveserver2');
        CREATE USER MAPPING FOR postgres server hdfs_svr OPTIONS (username 'ldapadm', password 'ldapadm');
        CREATE FOREIGN TABLE f_names_tab( a int, name varchar(255)) SERVER hdfs_svr
        OPTIONS (dbname 'testdb', table_name 'my_names_tab');
    ```
  
Please note that we are using the same port and client_type while creating foreign server because Spark Thrift Server is compatible with Hive Thrift Server. Applications using Hiveserver2 would work with Spark without any code changes.

5. Download & install Apache Spark in local mode

6. Test Spark installation using spark shell
    ```sql
    ./spark-shell
    Spark session available as 'spark'.
    Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.0
      /_/
        
    Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_111)
    Type in expressions to have them evaluated.
    Type :help for more information.

    scala> val no = Array(1, 2, 3, 4, 5,6,7,8,9,10)
    no: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    scala> val noData = sc.parallelize(no)
    scala> noData.sum
    res0: Double = 55.0
    ```
7. In the folder ``$SPARK_HOME/conf`` create a file ``spark-defaults.conf`` containing the following line
    ```sql
    spark.sql.warehouse.dir hdfs://localhost:9000/user/hive/warehouse
    ```

   By default spark uses derby for both meta data and the data itself (called warehouse in spark)
   In order to have spark use hadoop as warehouse we have to add this property.

8. Start Spark Thrift Server
    ```sql
    ./start-thriftserver.sh
    ```

9. Make sure Spark thrift server is running using log file

10. Run the following commands in beeline command line tool

  ```sql
  ./beeline
  Beeline version 1.0.1 by Apache Hive
  beeline> !connect jdbc:hive2://localhost:10000 'ldapadm' 'abcdef'  org.apache.hive.jdbc.HiveDriver
  Connecting to jdbc:hive2://localhost:10000
  Connected to: Spark SQL (version 2.1.0)
  Driver: Hive JDBC (version 1.0.1)
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

  0: jdbc:hive2://localhost:10000> load data local inpath '/path/to/file/names.txt'
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

11. Run the following command in edb-psql
    ```sql
    select * from f_names_tab;
     a |  name 
    ---+--------
     1 | abcd
     2 | pqrs
     3 | wxyz
     4 | a_b_c
     5 | p_q_r
     0 |
    (6 rows)
    ```
Here are the corresponding files in hadoop


    ```sql
    $ hadoop fs -ls /user/hive/warehouse/
    Found 1 items
    drwxrwxr-x - user supergroup 0 2017-01-19 10:47 /user/hive/warehouse/my_test_db.db

    $ hadoop fs -ls /user/hive/warehouse/my_test_db.db/
    Found 1 items
    drwxrwxr-x - user supergroup 0 2017-01-19 10:50 /user/hive/warehouse/my_test_db.db/my_names_tab
    ```
    
How to build
-----
  
  See the file INSTALL for instructions on how to build and install
  the extension and it's dependencies. 
  
    
  
TODO
----
 1. Hadoop Installation Instructions
 2. Write-able support
 3. Flum support
  
Contributing
------------
If you experience any bug create new [issue][6] and if you have fix for that
create a pull request. Before submitting a bug-fix or new feature, please read
the [contributing guidelines][7].


Support
-------
This project will be modified to maintain compatibility with new PostgreSQL 
and EDB Postgres Advanced Server releases.
  
If you require commercial support, please contact the EnterpriseDB sales 
team, or check whether your existing PostgreSQL support provider can also 
support hdfs_fdw.
  
  
Copyright Information
---------------------
Copyright (c) 2011 - 2017, EnterpriseDB Corporation
  
Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph 
and the following two paragraphs appear in all copies.
  
  IN NO EVENT SHALL ENTERPRISEDB CORPORATION BE LIABLE TO ANY PARTY FOR
  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING 
  LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, 
  EVEN IF ENTERPRISEDB CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH 
  DAMAGE.
  
  ENTERPRISEDB CORPORATION SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
  BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
  A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, 
  AND ENTERPRISEDB CORPORATION HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, 
  SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
   
See the [`LICENSE`][10] file for full details.
  
[1]: http://www.apache.org/
[2]: http://hadoop.apache.org/releases.html
[3]: https://hive.apache.org/
[4]: https://hive.apache.org/downloads.html
[6]: https://github.com/EnterpriseDB/hdfs_fdw/issues/new
[7]: CONTRIBUTING.md
[8]: http://wiki.pentaho.com/download/attachments/23531451/weblogs_parse.zip?version=1&modificationDate=1327096242000
[9]: http://wiki.pentaho.com/display/BAD/Transforming+Data+within+Hive
[10]: LICENSE
[11]: http://spark.apache.org/


  
