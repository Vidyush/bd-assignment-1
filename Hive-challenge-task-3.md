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


4. Fetch the result of the select operation in your local as a csv file . 
5. Perform group by operation . 
7. Perform filter operation at least 5 kinds of filter examples . 
8. show and example of regex operation
9. alter table operation 
10 . drop table operation
12 . order by operation . 
13 . where clause operations you have to perform . 
14 . sorting operation you have to perform . 
15 . distinct operation you have to perform . 
16 . like an operation you have to perform . 
17 . union operation you have to perform . 
18 . table view operation you have to perform . 
