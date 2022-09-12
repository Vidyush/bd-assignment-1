1. The file used in the assignment -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

2. Load file into local cloudera storage from your local storage with the help of filezilla

### This command is used to move file from local location to hdfs location
3. hadoop fs -put /home/cloudera/data/sales_order_data.csv /tmp

### Create internal hive table using the above csv data by skipping the header 
4.  > create table sales_order_csv
    > (
    > o_num int,
    > q_ord int,
    > price double,
    > o_line_num int,
    > sales double,
    > status string,
    > qtr_id int,
    > month int,
    > year int,
    > prod string,
    > msrp int,
    > prod_code string,
    > phone string,
    > city string,
    > state string,
    > postal_code string,
    > country string,
    > territory string,
    > l_name string,
    > f_name string,
    > deal_size string
    > )
    > row format delimited
    > fields terminated by ','
    > tblproperties ("skip.header.line.count"="1");

### Load data from HDFS path into the sales_order_csv internal table
5.load data inpath '/tmp/sales_order_data.csv' into table sales_order_csv;

### Creating an internal hive table for orc file format
6.  create table sales_order_orc
(
o_num int,
q_ord int,
price double,
o_line_num int,
sales double,
status string,
qtr_id int,
month int,
year int,
prod string,
msrp int,
prod_code string,
phone string,
city string,
state string,
postal_code string,
country string,
territory string,
l_name string,
f_name string,
deal_size string
)
stored as orc;

### Load data from "sales_order_csv" into "sales_order_orc"
7. from sales_order_csv insert overwrite table sales_order_orc select *;

### HQL commands on ORC file format data.

### Calculate the total sales per year?
a. select year,sum(sales) from sales_order_orc group by year;

### Output->
Query ID = cloudera_20220912133838_3a39dfd8-1601-42a0-b03b-24398a1ee47a
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0006, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0006/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 13:39:16,924 Stage-1 map = 0%,  reduce = 0%
2022-09-12 13:39:38,210 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.09 sec
2022-09-12 13:39:59,154 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 6.76 sec
MapReduce Total cumulative CPU time: 6 seconds 760 msec
Ended Job = job_1662975467880_0006
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 6.76 sec   HDFS Read: 38011 HDFS Write: 62 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 760 msec
OK
2003    3516979.540000001
2004    4724162.599999997
2005    1791486.71

### Find a product for which the maximum orders were placed?
b. select prod,sum(q_ord) as total_quantity from sales_order_orc group by prod order by total_quantity;

### Output->
Query ID = cloudera_20220912134848_1db0a334-08a9-437d-9408-9585e83d179e
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0008, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0008/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0008
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 13:48:30,961 Stage-1 map = 0%,  reduce = 0%
2022-09-12 13:48:40,962 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.74 sec
2022-09-12 13:48:53,202 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.63 sec
MapReduce Total cumulative CPU time: 3 seconds 630 msec
Ended Job = job_1662975467880_0008
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0009, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0009/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0009
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-12 13:49:14,677 Stage-2 map = 0%,  reduce = 0%
2022-09-12 13:49:33,268 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 2.28 sec
2022-09-12 13:49:54,706 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 6.06 sec
MapReduce Total cumulative CPU time: 6 seconds 60 msec
Ended Job = job_1662975467880_0009
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.63 sec   HDFS Read: 28059 HDFS Write: 311 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 6.06 sec   HDFS Read: 5245 HDFS Write: 115 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 690 msec
OK
prod    total_quantity
Trains  2712
Ships   8127
Planes  10727
Trucks and Buses        10777
Motorcycles     11663
Vintage Cars    21069
Classic Cars    33992











