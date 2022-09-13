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

### Calulate the total sales for each quarter
c. select qtr_id, sum(sales) as total_sales from sales_order_orc group by qtr_id order by qtr_id asc;
    
### Output->
Query ID = cloudera_20220912140101_27d5c7a0-36c1-4575-8bb2-55dc9a125f14
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0012, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0012/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0012
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 14:01:31,572 Stage-1 map = 0%,  reduce = 0%
2022-09-12 14:01:50,054 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.89 sec
2022-09-12 14:02:08,671 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 6.13 sec
MapReduce Total cumulative CPU time: 6 seconds 130 msec
Ended Job = job_1662975467880_0012
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0013, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0013/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0013
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-12 14:02:33,105 Stage-2 map = 0%,  reduce = 0%
2022-09-12 14:02:49,609 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 2.29 sec
2022-09-12 14:03:14,259 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 5.95 sec
MapReduce Total cumulative CPU time: 5 seconds 950 msec
Ended Job = job_1662975467880_0013
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 6.13 sec   HDFS Read: 37282 HDFS Write: 200 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.95 sec   HDFS Read: 5122 HDFS Write: 76 SUCCESS
Total MapReduce CPU Time Spent: 12 seconds 80 msec
OK
qtr_id  total_sales
1       2350817.7300000004
2       2048120.2999999986
3       1758910.8099999994
4       3874780.01

### In which quarter sales was minimum? [In third quarter sales was minimum]
d. select qtr_id, sum(sales) as total_sales from sales_order_orc group by qtr_id order by total_sales asc limit 1;

### Output->
Query ID = cloudera_20220912142323_e8886342-2025-4c84-9658-6e9e916ce388
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0014, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0014/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0014
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 14:23:14,270 Stage-1 map = 0%,  reduce = 0%
2022-09-12 14:23:24,241 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.49 sec
2022-09-12 14:23:35,108 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.19 sec
MapReduce Total cumulative CPU time: 3 seconds 190 msec
Ended Job = job_1662975467880_0014
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0015, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0015/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0015
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-12 14:23:47,923 Stage-2 map = 0%,  reduce = 0%
2022-09-12 14:23:56,582 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.22 sec
2022-09-12 14:24:07,494 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.11 sec
MapReduce Total cumulative CPU time: 3 seconds 110 msec
Ended Job = job_1662975467880_0015
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.19 sec   HDFS Read: 37283 HDFS Write: 200 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.11 sec   HDFS Read: 5243 HDFS Write: 21 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 300 msec
OK
qtr_id  total_sales
3       1758910.8099999994
    
### In which country sales was maximum? [In USA sales was maximum]
e. select country, sum(sales) as total_sales from sales_order_orc group by country order by total_sales desc limit 1;;

### Output->
Query ID = cloudera_20220912143636_a2a03372-5cc9-4f76-8fb1-2e1ba9e863b7
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0018, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0018/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0018
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 14:36:21,648 Stage-1 map = 0%,  reduce = 0%
2022-09-12 14:36:32,747 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.5 sec
2022-09-12 14:36:44,780 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.19 sec
MapReduce Total cumulative CPU time: 3 seconds 190 msec
Ended Job = job_1662975467880_0018
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0019, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0019/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0019
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-12 14:36:57,762 Stage-2 map = 0%,  reduce = 0%
2022-09-12 14:37:06,427 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.27 sec
2022-09-12 14:37:17,437 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.15 sec
MapReduce Total cumulative CPU time: 3 seconds 150 msec
Ended Job = job_1662975467880_0019
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.19 sec   HDFS Read: 38159 HDFS Write: 716 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.15 sec   HDFS Read: 5781 HDFS Write: 15 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 340 msec
OK
country total_sales
USA     3627982.83
    
### In which country sales was minimum? [In USA sales was minimum]
e(2).  select country, sum(sales) as total_sales from sales_order_orc group by country order by total_sales asc limit 1;

### Output->
Query ID = cloudera_20220912144040_df2cd7ef-8ad2-4eba-b44a-63f0e56a84ae
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0020, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0020/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0020
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 14:41:08,558 Stage-1 map = 0%,  reduce = 0%
2022-09-12 14:41:18,379 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.59 sec
2022-09-12 14:41:30,249 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.29 sec
MapReduce Total cumulative CPU time: 3 seconds 290 msec
Ended Job = job_1662975467880_0020
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0021, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0021/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0021
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-12 14:41:43,700 Stage-2 map = 0%,  reduce = 0%
2022-09-12 14:41:53,622 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.33 sec
2022-09-12 14:42:07,605 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.23 sec
MapReduce Total cumulative CPU time: 3 seconds 230 msec
Ended Job = job_1662975467880_0021
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.29 sec   HDFS Read: 38158 HDFS Write: 716 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.23 sec   HDFS Read: 5772 HDFS Write: 17 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 520 msec
OK
country total_sales
Ireland 57756.43

### Calculate the quaterly sales for each city?
f. select city,qtr_id, sum(sales) from sales_order_orc group by city,qtr_id;
    
### Output->
Query ID = cloudera_20220912144949_fa000cb4-8daa-4187-ad02-aebc339e3ee7
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1662975467880_0023, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662975467880_0023/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662975467880_0023
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-12 14:49:19,197 Stage-1 map = 0%,  reduce = 0%
2022-09-12 14:49:27,860 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.6 sec
2022-09-12 14:49:38,667 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.53 sec
MapReduce Total cumulative CPU time: 3 seconds 530 msec
Ended Job = job_1662975467880_0023
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.53 sec   HDFS Read: 40211 HDFS Write: 4361 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 530 msec
OK
city    qtr_id  _c2
Aaarhus 4       100595.54999999999
Allentown       2       6166.8
Allentown       3       71930.61
Allentown       4       44040.73
Barcelona       2       4219.2
Barcelona       4       74192.66000000002
Bergamo 1       56181.32
Bergamo 4       81774.4
Bergen  3       16363.1
Bergen  4       95277.18000000001
Boras   1       31606.72
Boras   3       53941.69
Boras   4       48710.92
Boston  2       74994.24
Boston  3       15344.640000000001
Boston  4       63730.780000000006
Brickhaven      1       31474.78
Brickhaven      2       7277.35
Brickhaven      3       114974.54000000001
Brickhaven      4       11528.53
Bridgewater     2       75778.99
Bridgewater     4       26115.800000000003
Brisbane        1       16118.48
Brisbane        3       34100.03
Bruxelles       1       18800.09
Bruxelles       2       8411.95
Bruxelles       3       47760.48
Burbank 1       37850.079999999994
Burbank 4       8234.560000000001
Burlingame      1       13529.57
Burlingame      3       42031.83
Burlingame      4       65221.66999999999
Cambridge       1       21782.699999999997
Cambridge       2       14380.92
Cambridge       3       48828.72
Cambridge       4       54251.66
Charleroi       1       16628.16
Charleroi       2       1711.26
Charleroi       3       1637.2
Charleroi       4       13463.48
Chatswood       2       43971.43
Chatswood       3       69694.40000000001
Chatswood       4       37905.149999999994
Cowes   1       26906.68
Cowes   4       51334.16
Dublin  1       38784.47
Dublin  3       18971.96
Espoo   1       51373.490000000005
Espoo   2       31018.230000000003
Espoo   3       31569.429999999993
Frankfurt       1       48698.83
Frankfurt       4       36472.76
Gensve  1       50432.55
Gensve  3       67281.01000000001
Glen Waverly    2       14378.09
Glen Waverly    3       12334.82
Glen Waverly    4       37878.55
Glendale        1       3987.2
Glendale        2       20350.95
Glendale        3       7600.12
Glendale        4       34485.50000000001
Graz    1       8775.16
Graz    4       43488.73999999999
Helsinki        1       26422.82
Helsinki        3       42744.06
Helsinki        4       42083.5
Kobenhavn       1       58871.11
Kobenhavn       2       62091.880000000005
Kobenhavn       4       24078.610000000004
Koln    4       100306.58
Las Vegas       2       33847.619999999995
Las Vegas       3       34453.85
Las Vegas       4       14449.61
Lille   1       20178.129999999997
Lille   4       48874.280000000006
Liverpool       2       91211.05999999998
Liverpool       4       26797.210000000003
London  1       8477.220000000001
London  2       32376.289999999997
London  4       83970.03
Los Angeles     1       23889.32
Los Angeles     4       24159.14
Lule    1       9749.0
Lule    4       66005.88
Lyon    1       101339.14000000001
Lyon    4       41535.10999999999
Madrid  1       357668.48999999993
Madrid  2       339588.0500000001
Madrid  3       69714.09
Madrid  4       315580.8100000001
Makati City     1       55245.020000000004
Makati City     4       38770.71000000001
Manchester      1       51017.91999999999
Manchester      4       106789.89
Marseille       1       2317.44
Marseille       2       52481.840000000004
Marseille       4       20136.859999999997
Melbourne       1       49637.57
Melbourne       2       60135.840000000004
Melbourne       4       91222.00000000001
Minato-ku       1       38191.39
Minato-ku       2       26482.700000000004
Minato-ku       4       55888.65000000001
Montreal        2       58257.50000000001
Montreal        4       15947.29
Munich  3       34993.92
NYC     1       32647.81
NYC     2       165100.34
NYC     3       63027.919999999984
NYC     4       300011.6999999999
Nantes  1       59617.4
Nantes  2       60344.98999999999
Nantes  3       61310.88000000002
Nantes  4       23031.589999999997
Nashua  1       12133.25
Nashua  4       119552.05000000002
New Bedford     1       48578.96
New Bedford     3       45738.39
New Bedford     4       113557.50999999997
New Haven       2       36973.310000000005
New Haven       4       42498.76
Newark  1       8722.12
Newark  2       74506.06999999998
North Sydney    1       65012.42
North Sydney    3       47191.76
North Sydney    4       41791.95
Osaka   1       50490.64
Osaka   2       17114.43
Oslo    3       34145.47
Oslo    4       45078.759999999995
Oulu    1       49055.4
Oulu    2       17813.4
Oulu    3       37501.58
Paris   1       71494.18
Paris   2       80215.41999999998
Paris   3       27798.480000000003
Paris   4       89436.59999999999
Pasadena        1       44273.35999999999
Pasadena        3       55776.11999999998
Pasadena        4       4512.48
Philadelphia    1       27398.82
Philadelphia    2       7287.24
Philadelphia    4       116503.06999999999
Reggio Emilia   2       41509.94
Reggio Emilia   3       56421.65000000001
Reggio Emilia   4       44669.740000000005
Reims   1       52029.07000000001
Reims   2       18971.96
Reims   3       15146.319999999998
Reims   4       48895.59
Salzburg        2       98104.23999999999
Salzburg        3       6693.280000000001
Salzburg        4       45001.11000000001
San Diego       1       87489.22999999998
San Francisco   1       72899.19999999998
San Francisco   4       151459.48
San Jose        2       160010.26999999996
San Rafael      1       267315.26
San Rafael      2       7261.75
San Rafael      3       216297.4
San Rafael      4       163983.65
Sevilla 4       54723.62
Singapore       1       28395.19
Singapore       2       92033.77000000002
Singapore       3       90250.08
Singapore       4       77809.37000000001
South Brisbane  1       21730.03
South Brisbane  3       10640.29
South Brisbane  4       27098.800000000003
Stavern 1       54701.99999999999
Stavern 4       61897.19
Strasbourg      2       80438.48
Torino  3       94117.26000000002
Toulouse        1       15139.119999999999
Toulouse        3       17251.08
Toulouse        4       38098.240000000005
Tsawassen       2       31302.5
Tsawassen       3       43332.350000000006
Vancouver       4       75238.92
Versailles      1       5759.42
Versailles      4       59074.9
White Plains    4       85555.98999999998

### Find a month for each year in which maximum number of quantities were sold?
h. with cte as (select year,month, sum(sales) as tot_sales, row_number() over(partition by year order by sum(sales) desc) as row_num from sales_order_orc group by year,month) select year,month,tot_sales from cte where row_num=1;
    
### Output
Query ID = cloudera_20220913045555_cab9f6f0-8be2-46ed-8b51-b9a63573c7e6
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0006, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0006/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-13 04:55:24,508 Stage-1 map = 0%,  reduce = 0%
2022-09-13 04:55:36,781 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.93 sec
2022-09-13 04:55:49,146 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.76 sec
MapReduce Total cumulative CPU time: 3 seconds 760 msec
Ended Job = job_1663066990219_0006
Launching Job 2 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0007, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0007/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0007
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-13 04:56:02,404 Stage-2 map = 0%,  reduce = 0%
2022-09-13 04:56:12,296 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.32 sec
2022-09-13 04:56:24,513 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.84 sec
MapReduce Total cumulative CPU time: 3 seconds 840 msec
Ended Job = job_1663066990219_0007
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.76 sec   HDFS Read: 38117 HDFS Write: 937 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.84 sec   HDFS Read: 8694 HDFS Write: 80 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 600 msec
OK
2003    11      1029837.6600000001
2004    11      1089048.0100000005
2005    5       457861.05999999965











