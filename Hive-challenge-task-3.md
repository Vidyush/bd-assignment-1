### Q1. Will the reducer work or not if you use “Limit 1” in any HiveQL query?

A1. The reducer will not work when limit is invoked for a normal read query. But if the limit is used with aggregate functions or joins or group by or order by then they will require reducer but limit still will not require reducer.
------------------------------------------------------------------->

### Q2. Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. Then, what will happen if we have multiple clients trying to access Hive at the same time?

A2. The default metastore configuration is stored in derby. And if multiple clients will try to access it at the same time it will not accept any incoming request as one one connection is entertained at one time. That is one of the drawback for using default metastore configuration and one should use oracle or postgress for instead of derby for the same. And even if you have to install hive again you metastore data will be lost, so having an external metastore is very beneficial(RDBMS).
------------------------------------------------------------------->

### Q3. Suppose, I create a table that contains details of all the transactions done by the customers: CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
### Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. How will you solve this problem and list the steps that I will be taking in order to do so?


BUILD A DATA PIPELINE WITH HIVE

Download a data from the given location - 
https://archive.ics.uci.edu/ml/machine-learning-databases/00360/

### 1. Create a hive table as per given schema in your dataset 
### Answer-> 
> create table air_quality_csv
    > (
    > date string,
    > time string,
    > co string,
    > pts1 string,
    > nmhc string,
    > c6h6 string,
    > pts2 string,
    > no string,
    > pts3 string,
    > no2 string,
    > pts4 string,
    > pts5 string,
    > t string,
    > rh string,
    > ah string
    > )
    > row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    > with serdeproperties (
    > "separatorChar" = '\;',
    > "quoteChar" = "\"",
    > "escapeChar"="\\"
    > )
    > stored as textfile
    > tblproperties ("skip.header.line.count"="1");

### 2. try to place a data into table location
### Answer->
> load data local inpath 'file:///home/cloudera/data/AirQualityUCI.csv' into table air_quality_csv;
Loading data to table default.air_quality_csv

### 3. Perform a select operation . 
### Answer-> 
> select * from air_quality_csv limit 5;
OK
10/03/2004      18.00.00        2,6     1360    150     11,9    1046    166     10     56      113     1692    1268    13,6    48,9    0,7578
10/03/2004      19.00.00        2       1292    112     9,4     955     103     11     74      92      1559    972     13,3    47,7    0,7255
10/03/2004      20.00.00        2,2     1402    88      9,0     939     131     11     40      114     1555    1074    11,9    54,0    0,7502
10/03/2004      21.00.00        2,2     1376    80      9,2     948     172     10     92      122     1584    1203    11,0    60,0    0,7867
10/03/2004      22.00.00        1,6     1272    51      6,5     836     131     12     05      116     1490    1110    11,2    59,6    0,7888

I saw that the above table is not proper like point values are having ,(comma) instead of .(decimal), So, for solving this i created a new table with the help of this query and casted changed all comma(,) with decimal(.) using regexp and casted that column to decimal(7,4). I also converted the date string column to a date format valid in hive so that multiple date function can be used like month() and year().
First i altered the table to change the column name from date to dat.

>alter table air_quality_csv change date dat string;

>create table air_quality_v1 as select cast(TO_DATE(from_unixtime(UNIX_TIMESTAMP(dat, 'dd/MM/yyyy')))as date) as dat,time,cast(regexp_replace(co,',','.') as decimal(7,4)) as co,pts1,nmhc,cast(regexp_replace(c6h6,',','.') as decimal(7,4)) as c6h6,pts2,no,pts3,no2,pts4,pts5,cast(regexp_replace(t,',','.') as decimal(7,4)) as t,cast(regexp_replace(rh,',','.') as decimal(7,4)) as rh,cast(regexp_replace(ah,',','.') as decimal(7,4)) as ah from air_quality_csv;
Query ID = cloudera_20220916072323_3d72525f-41c0-44d2-978b-2ea8e3e39a1b
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1663066990219_0024, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0024/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0024
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-09-16 07:23:22,427 Stage-1 map = 0%,  reduce = 0%
2022-09-16 07:23:36,686 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.98 sec
MapReduce Total cumulative CPU time: 5 seconds 980 msec
Ended Job = job_1663066990219_0024
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/hive_class_b1.db/.hive-staging_hive_2022-09-16_07-23-06_549_5058299315403323349-1/-ext-10001
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/hive_class_b1.db/air_quality_v1
Table hive_class_b1.air_quality_v1 stats: [numFiles=1, numRows=9471, totalSize=750058, rawDataSize=740587]
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1   Cumulative CPU: 5.98 sec   HDFS Read: 791678 HDFS Write: 750148 SUCCESS
Total MapReduce CPU Time Spent: 5 seconds 980 msec
OK
dat     time    co      pts1    nmhc    c6h6    pts2    no      pts3    no2     pts4    pts5    t       rh      ah
Time taken: 31.476 seconds

### 4. Fetch the result of the select operation in your local as a csv file . 
### Answer->
> insert overwrite local directory 'file:///home/cloudera/data/air' row format delimited fields terminated by ',' stored as textfile select * from air_quality_v1 limit 10;
> Query ID = cloudera_20220916075656_ecc8673d-c4d2-4588-9966-3d330290e153
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0028, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0028/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0028
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-16 07:56:21,974 Stage-1 map = 0%,  reduce = 0%
2022-09-16 07:56:34,411 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.08 sec
2022-09-16 07:56:53,298 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.48 sec
MapReduce Total cumulative CPU time: 4 seconds 480 msec
Ended Job = job_1663066990219_0028
Copying data to local directory file:/home/cloudera/data/air
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.48 sec   HDFS Read: 16465 HDFS Write: 777 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 480 msec
OK
air_quality_v1.dat      air_quality_v1.time     air_quality_v1.co       air_quality_v1.pts1     air_quality_v1.nmhc     air_quality_v1.c6h6        air_quality_v1.pts2     air_quality_v1.no       air_quality_v1.pts3     air_quality_v1.no2      air_quality_v1.pts4        air_quality_v1.pts5     air_quality_v1.t        air_quality_v1.rh       air_quality_v1.ah
Time taken: 47.021 seconds

### 5. Perform group by operation . 
### Answer->
> select year(dat) as year,month(dat) as month,sum(pts1) as tot_pts1 from air_quality_v1 group by year(dat),month(dat) order by year,month;
> Query ID = cloudera_20220916080101_156ff928-0d4b-456d-8f67-629e5149e618
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Defaulting to jobconf value of: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0029, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0029/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0029
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 3
2022-09-16 08:02:08,359 Stage-1 map = 0%,  reduce = 0%
2022-09-16 08:02:20,683 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.1 sec
2022-09-16 08:02:50,399 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 6.99 sec
2022-09-16 08:02:51,450 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.99 sec
MapReduce Total cumulative CPU time: 8 seconds 990 msec
Ended Job = job_1663066990219_0029
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0030, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0030/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0030
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-16 08:03:09,702 Stage-2 map = 0%,  reduce = 0%
2022-09-16 08:03:19,507 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.37 sec
2022-09-16 08:03:30,423 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.3 sec
MapReduce Total cumulative CPU time: 3 seconds 300 msec
Ended Job = job_1663066990219_0030
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 3   Cumulative CPU: 8.99 sec   HDFS Read: 768202 HDFS Write: 719 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.3 sec   HDFS Read: 6640 HDFS Write: 236 SUCCESS
Total MapReduce CPU Time Spent: 12 seconds 290 msec
OK
year    month   tot_pts1
2004    3       623638.0
2004    4       800455.0
2004    5       783165.0
2004    6       688581.0
2004    7       777301.0
2004    8       672037.0
2004    9       755656.0
2004    10      880191.0
2004    11      815147.0
2004    12      705842.0
2005    1       746580.0
2005    2       633064.0
2005    3       850770.0
2005    4       82973.0
Time taken: 94.219 seconds, Fetched: 15 row(s)

### 7. Perform filter operation at least 5 kinds of filter examples . 
### ANswer->
> select dat from air_quality_v1 where month(dat) = 3;
> select dat, co from air_quality_v1 where co<0;
> select month(dat) as month from air_quality_v1 where month(dat) between 3 and 6;
> select month(dat) as month, year(dat), sum(co) from air_quality_v1 group by year(dat),month(dat) having sum(co)>1000;
> select month(dat) as month from air_quality_v1 where month(dat) is 3 or 10;

### 8. show and example of regex operation
### Answer->
> select distinct(regexp_replace(dat,'/','-')) as dat from air_quality_csv limit 4;
> Query ID = cloudera_20220916080808_2617cff7-890c-4843-8e68-c724a0279498
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Defaulting to jobconf value of: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0031, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0031/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0031
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 3
2022-09-16 08:08:29,726 Stage-1 map = 0%,  reduce = 0%
2022-09-16 08:08:40,826 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.73 sec
2022-09-16 08:09:10,224 Stage-1 map = 100%,  reduce = 33%, Cumulative CPU 5.09 sec
2022-09-16 08:09:11,337 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 7.2 sec
2022-09-16 08:09:12,392 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.27 sec
MapReduce Total cumulative CPU time: 9 seconds 270 msec
Ended Job = job_1663066990219_0031
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 3   Cumulative CPU: 9.27 sec   HDFS Read: 803053 HDFS Write: 155 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 270 msec
OK
dat
01-03-2005
01-04-2004
01-07-2004
01-10-2004
Time taken: 54.557 seconds, Fetched: 4 row(s)

### 9. alter table operation 
### Answer->
> alter table air_quality_csv change date dat string;

### 10 . drop table operation
### Answer->
> drop table air_temp; // this is temporary table i created to play around with the data and check regexp.
 
### 12 . order by operation . 
### Answer->
> select year(dat) as year,month(dat) as month,sum(pts1) as tot_pts1 from air_quality_v1 group by year(dat),month(dat) order by year,month;

### 13 . where clause operations you have to perform . 
### Answer->
> select year(dat),month(dat), no from air_quality_v1 where month(dat) = 3;

### 14 . sorting operation you have to perform . 
### Answer->
> select year(dat) as year,sum(pts1) as tot_pts1 from air_quality_v1 group by year(dat) order by year desc;

### 15 . distinct operation you have to perform . 
### Answer->
> select distinct(year(dat)) as year from air_quality_v1;
Query ID = cloudera_20220916111515_ab1a4729-4eb2-4e19-8b8a-f1def7bf88c6
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Defaulting to jobconf value of: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0032, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0032/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0032
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 3
2022-09-16 11:15:41,292 Stage-1 map = 0%,  reduce = 0%
2022-09-16 11:15:53,111 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.67 sec
2022-09-16 11:16:22,676 Stage-1 map = 100%,  reduce = 33%, Cumulative CPU 5.09 sec
2022-09-16 11:16:23,875 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 7.19 sec
2022-09-16 11:16:24,930 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.87 sec
MapReduce Total cumulative CPU time: 8 seconds 870 msec
Ended Job = job_1663066990219_0032
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 3   Cumulative CPU: 8.87 sec   HDFS Read: 768120 HDFS Write: 13 SUCCESS
Total MapReduce CPU Time Spent: 8 seconds 870 msec
OK
year
2004
2005
Time taken: 60.815 seconds, Fetched: 2 row(s)
    
### 16 . like an operation you have to perform .
### Answer->
> select dat, pts3,nmhc from air_quality_csv where dat like '_%1__2004' limit 10;

###17 . union operation you have to perform . 
### ANswer->
> select dat from air_quality_v1
> union all
> select dat from air_quality_view;

    
### 18 . table view operation you have to perform . 
### ANswer->
>  create view air_quality_view as select dat, time, cast(regexp_replace(co, ',', '.') as decimal(7,4)) as co from air_quality_v1;

