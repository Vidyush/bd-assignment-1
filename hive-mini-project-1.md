## 1. agent login report schema and data load

hive> create table agent_login_report(slid int,agent string, dat string, login string, logout string, duration string) row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES ("skip.header.line.count"="1");

hive> load data local inpath 'file:///home/cloudera/hive-project1/AgentLogingReport.csv' into table agent_login_report;

## agent perfromance schema and data load

hive> create table agent_performance(slid int,dat string,agent string, tot_chat int, avg_rsp_time string, avg_resol_time string, avg_rating double,tot_feedback int) row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES ("skip.header.line.count"="1");

hive> load data local inpath 'file:///home/cloudera/hive-project1/AgentPerformance.csv' into table agent_performance;

## 2. List of all agents' names. 

hive> select count(distinct(agent)) from agent_performance;
agent performance table: 70

hive> select count(distinct(agent)) from agent_login_report;
agent login table: 49

##3. Find out agent average rating.

hive> select agent, round(avg(avg_rating),2) as avg_rating from agent_performance group by agent order by avg_rating desc;
Query ID = cloudera_20220920134242_3367ca19-cb66-4b8e-a128-3e3c2b6077bf
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0054, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0054/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0054
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-20 13:42:25,441 Stage-1 map = 0%,  reduce = 0%
2022-09-20 13:42:35,409 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.6 sec
2022-09-20 13:42:47,482 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.87 sec
MapReduce Total cumulative CPU time: 3 seconds 870 msec
Ended Job = job_1663066990219_0054
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0055, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0055/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0055
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-20 13:43:00,338 Stage-2 map = 0%,  reduce = 0%
2022-09-20 13:43:09,109 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.21 sec
2022-09-20 13:43:21,043 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.08 sec
MapReduce Total cumulative CPU time: 3 seconds 80 msec
Ended Job = job_1663066990219_0055
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.87 sec   HDFS Read: 118359 HDFS Write: 2733 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.08 sec   HDFS Read: 7512 HDFS Write: 1193 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 950 msec
OK
agent   avg_rating
Shivananda Sonwane      4.23
Khushboo Priya  3.7
Manjunatha A    3.59
Boktiar Ahmed Bappy     3.57
Ishawant Kumar  3.54
Madhulika G     3.5
Ayushi Mishra   3.48
Jawala Prakash  3.47
Sanjeev Kumar   3.38
Nishtha Jain    3.28
Prerna Singh    3.23
Shubham Sharma  3.23
Jaydeep Dixit   3.17
Hrisikesh Neogi 3.14
Bharath         2.98
Maitry  2.93
Nandani Gupta   2.92
Deepranjan Gupta        2.89
Shivan K        2.84
Harikrishnan Shaji      2.64
Prabir Kumar Satapathy  2.51
Prateek _iot    2.44
Swati   2.42
Mahesh Sarade   2.4
Wasim   2.4
Mithun S        2.36
Aditya_iot      2.35
Zeeshan         2.29
Ameya Jain      2.22
Aravind         2.18
Saikumarreddy N 1.98
Aditya Shinde   1.8
Rishav Dash     1.43
Sowmiya Sivakumar       1.26
Jayant Kumar    1.07
Shiva Srivastava        0.94
Chaitra K Hiremath      0.86
Muskan Garg     0.71
Anirudh         0.65
Saurabh Shukla  0.56
Vivek   0.5
Sandipan Saha   0.43
Sudhanshu Kumar 0.33
Suraj S Bilgi   0.31
Mukesh  0.31
Ankitjha        0.27
Mukesh Rao      0.26
Anurag Tiwari   0.18
Ashad Nasim     0.17
Maneesh         0.17
Shivan_S        0.14
Mahak   0.1
Tarun   0.05
Nitin M 0.0
Rohan   0.0
Saif Khan       0.0
Samprit         0.0
Sanjeevan       0.0
Ineuron Intelligence    0.0
Hyder Abbas     0.0
Spuri   0.0
Hitesh Choudhary        0.0
Dibyanshu       0.0
Uday Mishra     0.0
Vasanth P       0.0
Ashish  0.0
Ankit Sharma    0.0
Amersh  0.0
Aditya  0.0
Abhishek        0.0
Time taken: 69.985 seconds, Fetched: 70 row(s)

## 4. Total working days for each agents

# from agent login report

hive> select agent,count(distinct(dat)) as no_of_days from agent_login_report group by agent order by no_of_days desc;

Query ID = cloudera_20220921130808_b34fde34-9861-417d-bc79-8d5f7c3c5b7e
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0073, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0073/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0073
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-21 13:08:28,535 Stage-1 map = 0%,  reduce = 0%
2022-09-21 13:08:39,842 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.63 sec
2022-09-21 13:08:56,069 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.45 sec
MapReduce Total cumulative CPU time: 3 seconds 450 msec
Ended Job = job_1663066990219_0073
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0074, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0074/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0074
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-21 13:09:10,281 Stage-2 map = 0%,  reduce = 0%
2022-09-21 13:09:20,210 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.32 sec
2022-09-21 13:09:32,439 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.26 sec
MapReduce Total cumulative CPU time: 3 seconds 260 msec
Ended Job = job_1663066990219_0074
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.45 sec   HDFS Read: 62958 HDFS Write: 1616 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.26 sec   HDFS Read: 6550 HDFS Write: 742 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 710 msec
OK
agent   no_of_days
Ishawant Kumar  11
Prateek _iot    11
Shubham Sharma  11
Anurag Tiwari   10
Deepranjan Gupta        10
Shivananda Sonwane      10
Hrisikesh Neogi 9
Zeeshan 9
Dibyanshu       9
Nandani Gupta   9
Ayushi Mishra   9
Boktiar Ahmed Bappy     9
Sanjeev Kumar   9
Wasim   9
Jawala Prakash  9
Prerna Singh    9
Harikrishnan Shaji      9
Madhulika G     8
Khushboo Priya  8
Bharath 8
Sowmiya Sivakumar       8
Shivan K        8
Shiva Srivastava        8
Aditya_iot      8
Nishtha Jain    8
Mithun S        8
Mahesh Sarade   8
Manjunatha A    7
Saikumarreddy N 7
Rishav Dash     7
Prabir Kumar Satapathy  7
Jaydeep Dixit   7
Chaitra K Hiremath      7
Aravind 7
Ameya Jain      7
Sudhanshu Kumar 6
Muskan Garg     6
Maitry  5
Swati   4
Saurabh Shukla  4
Mukesh  2
Amersh  2
Suraj S Bilgi   2
Ankitjha        2
Hyder Abbas     2
Aditya Shinde   1
Ineuron Intelligence    1
Nitin M 1
Tarun   1

## 5. from agent perfrormance

hive> select agent,count(distinct(dat)) as no_of_days from agent_performance group by agent order by no_of_days desc;
Query ID = cloudera_20220921133434_ad82e223-0f4e-4b70-abaf-9e30fe955ba0
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0075, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0075/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0075
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-21 13:34:26,449 Stage-1 map = 0%,  reduce = 0%
2022-09-21 13:34:43,093 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.25 sec
2022-09-21 13:35:04,642 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.44 sec
MapReduce Total cumulative CPU time: 4 seconds 440 msec
Ended Job = job_1663066990219_0075
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0076, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0076/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0076
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-21 13:35:20,522 Stage-2 map = 0%,  reduce = 0%
2022-09-21 13:35:31,633 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.41 sec
2022-09-21 13:35:46,321 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.5 sec
MapReduce Total cumulative CPU time: 3 seconds 500 msec
Ended Job = job_1663066990219_0076
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.44 sec   HDFS Read: 118016 HDFS Write: 2243 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.5 sec   HDFS Read: 7177 HDFS Write: 1077 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 940 msec
OK
agent   no_of_days
Zeeshan         30
Wasim   30
Vivek   30
Vasanth P       30
Uday Mishra     30
Tarun   30
Swati   30
Suraj S Bilgi   30
Sudhanshu Kumar 30
Spuri   30
Sowmiya Sivakumar       30
Shubham Sharma  30
Shivananda Sonwane      30
Shivan_S        30
Shivan K        30
Shiva Srivastava        30
Saurabh Shukla  30
Sanjeevan       30
Sanjeev Kumar   30
Sandipan Saha   30
Samprit         30
Saikumarreddy N 30
Saif Khan       30
Rohan   30
Rishav Dash     30
Prerna Singh    30
Prateek _iot    30
Prabir Kumar Satapathy  30
Nitin M 30
Nishtha Jain    30
Nandani Gupta   30
Muskan Garg     30
Mukesh Rao      30
Mukesh  30
Mithun S        30
Manjunatha A    30
Maneesh         30
Maitry  30
Mahesh Sarade   30
Mahak   30
Madhulika G     30
Khushboo Priya  30
Jaydeep Dixit   30
Jayant Kumar    30
Jawala Prakash  30
Ishawant Kumar  30
Ineuron Intelligence    30
Hyder Abbas     30
Hrisikesh Neogi 30
Hitesh Choudhary        30
Harikrishnan Shaji      30
Dibyanshu       30
Deepranjan Gupta        30
Chaitra K Hiremath      30
Boktiar Ahmed Bappy     30
Bharath         30
Ayushi Mishra   30
Ashish  30
Ashad Nasim     30
Aravind         30
Anurag Tiwari   30
Ankitjha        30
Ankit Sharma    30
Anirudh         30
Ameya Jain      30
Amersh  30
Aditya_iot      30
Aditya Shinde   30
Aditya  30
Abhishek        30


##6. Total query that each agent have taken 

hive> select agent,sum(tot_chat) as total_queries from agent_performance group by agent order by total_queries desc;
Query ID = cloudera_20220920135353_ccae015b-6016-41cf-8775-ee29330aeb11
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0059, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0059/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0059
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-20 13:53:36,977 Stage-1 map = 0%,  reduce = 0%
2022-09-20 13:53:46,793 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.53 sec
2022-09-20 13:53:58,819 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.26 sec
MapReduce Total cumulative CPU time: 3 seconds 260 msec
Ended Job = job_1663066990219_0059
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0060, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0060/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0060
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-20 13:54:10,648 Stage-2 map = 0%,  reduce = 0%
2022-09-20 13:54:19,424 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.26 sec
2022-09-20 13:54:31,272 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.19 sec
MapReduce Total cumulative CPU time: 3 seconds 190 msec
Ended Job = job_1663066990219_0060
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.26 sec   HDFS Read: 117756 HDFS Write: 2308 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.19 sec   HDFS Read: 7250 HDFS Write: 1088 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 450 msec
OK
agent   total_queries
Hrisikesh Neogi 578
Nandani Gupta   560
Zeeshan         542
Maitry  542
Swati   524
Ayushi Mishra   514
Jaydeep Dixit   512
Shubham Sharma  510
Sanjeev Kumar   507
Mithun S        503
Deepranjan Gupta        493
Madhulika G     469
Boktiar Ahmed Bappy     452
Khushboo Priya  446
Shivananda Sonwane      441
Jawala Prakash  439
Wasim   433
Manjunatha A    413
Rishav Dash     409
Prerna Singh    401
Harikrishnan Shaji      381
Nishtha Jain    373
Bharath         369
Aravind         366
Mahesh Sarade   364
Saikumarreddy N 364
Shivan K        357
Ishawant Kumar  338
Ameya Jain      322
Prabir Kumar Satapathy  299
Aditya Shinde   277
Aditya_iot      231
Sowmiya Sivakumar       206
Prateek _iot    190
Jayant Kumar    127
Anirudh         81
Chaitra K Hiremath      64
Muskan Garg     56
Shiva Srivastava        53
Vivek   44
Sandipan Saha   30
Suraj S Bilgi   28
Tarun   22
Mukesh  19
Ashad Nasim     18
Saurabh Shukla  16
Mahak   7
Shivan_S        7
Ankitjha        5
Mukesh Rao      5
Maneesh         4
Anurag Tiwari   4
Sudhanshu Kumar 2
Samprit         1
Hitesh Choudhary        1
Dibyanshu       1
Sanjeevan       0
Saif Khan       0
Rohan   0
Nitin M 0
Spuri   0
Ineuron Intelligence    0
Hyder Abbas     0
Uday Mishra     0
Vasanth P       0
Ashish  0
Ankit Sharma    0
Amersh  0
Aditya  0
Abhishek        0

##7. Total Feedback that each agent have received 

hive> select agent,sum(tot_feedback) as total_feedback from agent_performance group by agent order by total_feedback desc;
Query ID = cloudera_20220920135858_f4c98947-3b76-40b4-b344-b5da91d8340a
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0061, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0061/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0061
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-20 13:58:49,379 Stage-1 map = 0%,  reduce = 0%
2022-09-20 13:58:59,120 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.5 sec
2022-09-20 13:59:11,157 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.24 sec
MapReduce Total cumulative CPU time: 3 seconds 240 msec
Ended Job = job_1663066990219_0061
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0062, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0062/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0062
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-20 13:59:23,893 Stage-2 map = 0%,  reduce = 0%
2022-09-20 13:59:33,673 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.23 sec
2022-09-20 13:59:44,613 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.17 sec
MapReduce Total cumulative CPU time: 3 seconds 170 msec
Ended Job = job_1663066990219_0062
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.24 sec   HDFS Read: 117763 HDFS Write: 2295 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.17 sec   HDFS Read: 7247 HDFS Write: 1084 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 410 msec
OK
agent   total_feedback
Hrisikesh Neogi 367
Mithun S        364
Maitry  347
Zeeshan         335
Ayushi Mishra   329
Deepranjan Gupta        312
Boktiar Ahmed Bappy     311
Sanjeev Kumar   311
Nandani Gupta   308
Jaydeep Dixit   305
Swati   302
Shubham Sharma  300
Saikumarreddy N 290
Khushboo Priya  289
Wasim   284
Madhulika G     281
Rishav Dash     264
Shivananda Sonwane      263
Nishtha Jain    257
Manjunatha A    254
Jawala Prakash  250
Bharath         247
Shivan K        243
Prerna Singh    235
Aravind         233
Harikrishnan Shaji      231
Ameya Jain      228
Prabir Kumar Satapathy  222
Mahesh Sarade   216
Ishawant Kumar  202
Aditya Shinde   153
Sowmiya Sivakumar       141
Aditya_iot      131
Prateek _iot    107
Jayant Kumar    70
Shiva Srivastava        46
Anirudh         39
Chaitra K Hiremath      37
Muskan Garg     37
Vivek   20
Sandipan Saha   18
Mukesh  17
Suraj S Bilgi   15
Ashad Nasim     9
Saurabh Shukla  8
Tarun   6
Mahak   5
Mukesh Rao      5
Shivan_S        4
Anurag Tiwari   3
Ankitjha        3
Maneesh         3
Sudhanshu Kumar 2
Samprit         0
Saif Khan       0
Rohan   0
Sanjeevan       0
Nitin M 0
Spuri   0
Ineuron Intelligence    0
Hyder Abbas     0
Hitesh Choudhary        0
Dibyanshu       0
Uday Mishra     0
Vasanth P       0
Ashish  0
Ankit Sharma    0
Amersh  0
Aditya  0
Abhishek        0

##8. Agent name who have average rating between 3.5 to 4 

hive> select agent,avg(avg_rating) as a_rating from agent_performance group by agent having a_rating between 3.5 and 4 ;
Query ID = cloudera_20220920140707_20088cb3-452f-4fe8-97f7-8524c86ebed0
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0063, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0063/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0063
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-20 14:07:13,663 Stage-1 map = 0%,  reduce = 0%
2022-09-20 14:07:23,462 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.58 sec
2022-09-20 14:07:35,373 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.01 sec
MapReduce Total cumulative CPU time: 4 seconds 10 msec
Ended Job = job_1663066990219_0063
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.01 sec   HDFS Read: 119576 HDFS Write: 137 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 10 msec
OK
agent   a_rating
Boktiar Ahmed Bappy     3.5679999999999996
Ishawant Kumar  3.543333333333334
Khushboo Priya  3.703666666666666
Manjunatha A    3.5946666666666665

##9. Agent name who have rating less than 3.5

hive> select agent, avg_rating from agent_performance where avg_rating<3.5 order by avg_rating desc;
Time taken: 35.805 seconds, Fetched: 1474 row(s)
 
##10. Agent name who have rating more than 4.5

hive> select agent, avg_rating from agent_performance where avg_rating<3.5 order by avg_rating desc;
Time taken: 35.544 seconds, Fetched: 307 row(s)

##11. How many feedback agents have received more than 4.5 average

hive> select agent, sum(tot_feedback) as fb from agent_performance where avg_rating >4.5 group by agent order by fb;
Query ID = cloudera_20220921141010_60d39e87-8211-457f-b346-d60d12347757
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0086, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0086/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0086
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-21 14:10:38,520 Stage-1 map = 0%,  reduce = 0%
2022-09-21 14:10:49,593 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.13 sec
2022-09-21 14:11:00,530 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.01 sec
MapReduce Total cumulative CPU time: 4 seconds 10 msec
Ended Job = job_1663066990219_0086
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0087, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0087/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0087
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-09-21 14:11:14,110 Stage-2 map = 0%,  reduce = 0%
2022-09-21 14:11:25,162 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.36 sec
2022-09-21 14:11:38,733 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.47 sec
MapReduce Total cumulative CPU time: 3 seconds 470 msec
Ended Job = job_1663066990219_0087
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.01 sec   HDFS Read: 118593 HDFS Write: 1580 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.47 sec   HDFS Read: 6508 HDFS Write: 769 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 480 msec
OK
agent   fb
Sandipan Saha   1
Mukesh Rao      1
Ankitjha        1
Anirudh         2
Sudhanshu Kumar 2
Shiva Srivastava        2
Saurabh Shukla  3
Vivek   3
Suraj S Bilgi   4
Chaitra K Hiremath      5
Jayant Kumar    7
Muskan Garg     14
Sowmiya Sivakumar       16
Mukesh  17
Nishtha Jain    18
Rishav Dash     27
Jawala Prakash  33
Aditya_iot      43
Mahesh Sarade   46
Zeeshan         48
Boktiar Ahmed Bappy     52
Prabir Kumar Satapathy  53
Prateek _iot    54
Madhulika G     67
Aditya Shinde   73
Ayushi Mishra   75
Ishawant Kumar  79
Harikrishnan Shaji      80
Maitry  81
Prerna Singh    82
Deepranjan Gupta        83
Nandani Gupta   91
Mithun S        93
Swati   103
Shivan K        130
Manjunatha A    132
Khushboo Priya  134
Ameya Jain      150
Shivananda Sonwane      154
Shubham Sharma  155
Wasim   156
Sanjeev Kumar   164
Aravind         178
Jaydeep Dixit   179
Hrisikesh Neogi 183
Saikumarreddy N 184
Bharath         231

##12. average weekly response time for each agent 

hive> with cte as (select weekofyear(cast(to_date(from_unixtime(unix_timestamp(dat,'MM/dd/yyyy'))) as date)) as wk,agent,unix_timestamp(avg_rsp_time,'hh:mm:ss')-28800 as result from agent_performance)
    > select agent,wk,avg(result),from_unixtime(cast(round(avg(result),0) as int),'mm:ss') as pp from cte group by agent,wk;
Query ID = cloudera_20220921145050_77b7deee-6b84-414f-9900-3532ac27887d
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0090, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0090/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0090
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-21 14:50:35,954 Stage-1 map = 0%,  reduce = 0%
2022-09-21 14:50:47,898 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.98 sec
2022-09-21 14:50:58,844 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 5.38 sec
MapReduce Total cumulative CPU time: 5 seconds 380 msec
Ended Job = job_1663066990219_0090
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 5.38 sec   HDFS Read: 121828 HDFS Write: 10923 SUCCESS
Total MapReduce CPU Time Spent: 5 seconds 380 msec
OK
agent   wk      _c2     pp
Abhishek        26      0.0     00:00
Abhishek        27      0.0     00:00
Abhishek        28      0.0     00:00
Abhishek        29      0.0     00:00
Abhishek        30      0.0     00:00
Aditya  26      0.0     00:00
Aditya  27      0.0     00:00
Aditya  28      0.0     00:00
Aditya  29      0.0     00:00
Aditya  30      0.0     00:00
Aditya Shinde   26      55.333333333333336      00:55
Aditya Shinde   27      47.285714285714285      00:47
Aditya Shinde   28      56.42857142857143       00:56
Aditya Shinde   29      0.0     00:00
Aditya Shinde   30      0.0     00:00
Aditya_iot      26      0.0     00:00
Aditya_iot      27      20.142857142857142      00:20
Aditya_iot      28      52.0    00:52
Aditya_iot      29      30.142857142857142      00:30
Aditya_iot      30      50.5    00:51
Amersh  26      0.0     00:00
Amersh  27      0.0     00:00
Amersh  28      0.0     00:00
Amersh  29      0.0     00:00
Amersh  30      0.0     00:00
Ameya Jain      26      0.0     00:00
Ameya Jain      27      0.0     00:00
Ameya Jain      28      32.57142857142857       00:33
Ameya Jain      29      29.714285714285715      00:30
Ameya Jain      30      33.0    00:33
Anirudh         26      0.0     00:00
Anirudh         27      77.42857142857143       01:17
Anirudh         28      15.857142857142858      00:16
Anirudh         29      0.0     00:00
Anirudh         30      0.0     00:00
Ankit Sharma    26      0.0     00:00
Ankit Sharma    27      0.0     00:00
Ankit Sharma    28      0.0     00:00
Ankit Sharma    29      0.0     00:00
Ankit Sharma    30      0.0     00:00
Ankitjha        26      0.0     00:00
Ankitjha        27      0.0     00:00
Ankitjha        28      0.0     00:00
Ankitjha        29      0.0     00:00
Ankitjha        30      22.166666666666668      00:22
Anurag Tiwari   26      0.0     00:00
Anurag Tiwari   27      36.142857142857146      00:36
Anurag Tiwari   28      0.0     00:00
Anurag Tiwari   29      0.0     00:00
Anurag Tiwari   30      0.0     00:00
Aravind         26      0.0     00:00
Aravind         27      21.0    00:21
Aravind         28      34.142857142857146      00:34
Aravind         29      36.42857142857143       00:36
Aravind         30      0.0     00:00
Ashad Nasim     26      0.0     00:00
Ashad Nasim     27      165.57142857142858      02:46
Ashad Nasim     28      0.0     00:00
Ashad Nasim     29      0.0     00:00
Ashad Nasim     30      0.0     00:00
Ashish  26      0.0     00:00
Ashish  27      0.0     00:00
Ashish  28      0.0     00:00
Ashish  29      0.0     00:00
Ashish  30      0.0     00:00
Ayushi Mishra   26      57.0    00:57
Ayushi Mishra   27      50.142857142857146      00:50
Ayushi Mishra   28      48.285714285714285      00:48
Ayushi Mishra   29      85.28571428571429       01:25
Ayushi Mishra   30      58.833333333333336      00:59
Bharath         26      28.0    00:28
Bharath         27      24.285714285714285      00:24
Bharath         28      14.0    00:14
Bharath         29      45.0    00:45
Bharath         30      22.833333333333332      00:23
Boktiar Ahmed Bappy     26      60.333333333333336      01:00
Boktiar Ahmed Bappy     27      42.857142857142854      00:43
Boktiar Ahmed Bappy     28      59.0    00:59
Boktiar Ahmed Bappy     29      66.85714285714286       01:07
Boktiar Ahmed Bappy     30      103.16666666666667      01:43
Chaitra K Hiremath      26      0.0     00:00
Chaitra K Hiremath      27      0.0     00:00
Chaitra K Hiremath      28      0.0     00:00
Chaitra K Hiremath      29      32.714285714285715      00:33
Chaitra K Hiremath      30      37.5    00:38
Deepranjan Gupta        26      44.666666666666664      00:45
Deepranjan Gupta        27      59.42857142857143       00:59
Deepranjan Gupta        28      44.0    00:44
Deepranjan Gupta        29      46.285714285714285      00:46
Deepranjan Gupta        30      69.0    01:09
Dibyanshu       26      0.0     00:00
Dibyanshu       27      5.428571428571429       00:05
Dibyanshu       28      0.0     00:00
Dibyanshu       29      0.0     00:00
Dibyanshu       30      0.0     00:00
Harikrishnan Shaji      26      0.0     00:00
Harikrishnan Shaji      27      27.857142857142858      00:28
Harikrishnan Shaji      28      39.42857142857143       00:39
Harikrishnan Shaji      29      48.42857142857143       00:48
Harikrishnan Shaji      30      34.833333333333336      00:35
Hitesh Choudhary        26      0.0     00:00
Hitesh Choudhary        27      0.0     00:00
Hitesh Choudhary        28      0.0     00:00
Hitesh Choudhary        29      0.0     00:00
Hitesh Choudhary        30      0.0     00:00
Hrisikesh Neogi 26      37.666666666666664      00:38
Hrisikesh Neogi 27      46.142857142857146      00:46
Hrisikesh Neogi 28      45.285714285714285      00:45
Hrisikesh Neogi 29      58.0    00:58
Hrisikesh Neogi 30      59.333333333333336      00:59
Hyder Abbas     26      0.0     00:00
Hyder Abbas     27      0.0     00:00
Hyder Abbas     28      0.0     00:00
Hyder Abbas     29      0.0     00:00
Hyder Abbas     30      0.0     00:00
Ineuron Intelligence    26      0.0     00:00
Ineuron Intelligence    27      0.0     00:00
Ineuron Intelligence    28      0.0     00:00
Ineuron Intelligence    29      0.0     00:00
Ineuron Intelligence    30      0.0     00:00
Ishawant Kumar  26      0.0     00:00
Ishawant Kumar  27      39.714285714285715      00:40
Ishawant Kumar  28      65.28571428571429       01:05
Ishawant Kumar  29      62.714285714285715      01:03
Ishawant Kumar  30      55.0    00:55
Jawala Prakash  26      55.666666666666664      00:56
Jawala Prakash  27      110.14285714285714      01:50
Jawala Prakash  28      116.28571428571429      01:56
Jawala Prakash  29      73.85714285714286       01:14
Jawala Prakash  30      93.0    01:33
Jayant Kumar    26      63.0    01:03
Jayant Kumar    27      39.285714285714285      00:39
Jayant Kumar    28      12.714285714285714      00:13
Jayant Kumar    29      0.0     00:00
Jayant Kumar    30      0.0     00:00
Jaydeep Dixit   26      39.666666666666664      00:40
Jaydeep Dixit   27      51.42857142857143       00:51
Jaydeep Dixit   28      46.857142857142854      00:47
Jaydeep Dixit   29      43.285714285714285      00:43
Jaydeep Dixit   30      37.0    00:37
Khushboo Priya  26      80.0    01:20
Khushboo Priya  27      70.71428571428571       01:11
Khushboo Priya  28      58.42857142857143       00:58
Khushboo Priya  29      51.142857142857146      00:51
Khushboo Priya  30      56.166666666666664      00:56
Madhulika G     26      94.0    01:34
Madhulika G     27      60.285714285714285      01:00
Madhulika G     28      81.28571428571429       01:21
Madhulika G     29      49.57142857142857       00:50
Madhulika G     30      62.166666666666664      01:02
Mahak   26      0.0     00:00
Mahak   27      0.0     00:00
Mahak   28      0.0     00:00
Mahak   29      0.0     00:00
Mahak   30      0.0     00:00
Mahesh Sarade   26      0.0     00:00
Mahesh Sarade   27      38.57142857142857       00:39
Mahesh Sarade   28      55.42857142857143       00:55
Mahesh Sarade   29      59.0    00:59
Mahesh Sarade   30      53.666666666666664      00:54
Maitry  26      41.666666666666664      00:42
Maitry  27      48.42857142857143       00:48
Maitry  28      74.14285714285714       01:14
Maitry  29      70.85714285714286       01:11
Maitry  30      72.66666666666667       01:13
Maneesh         26      0.0     00:00
Maneesh         27      19.285714285714285      00:19
Maneesh         28      0.0     00:00
Maneesh         29      0.0     00:00
Maneesh         30      0.0     00:00
Manjunatha A    26      42.0    00:42
Manjunatha A    27      42.285714285714285      00:42
Manjunatha A    28      33.285714285714285      00:33
Manjunatha A    29      32.57142857142857       00:33
Manjunatha A    30      33.666666666666664      00:34
Mithun S        26      0.0     00:00
Mithun S        27      2.7142857142857144      00:03
Mithun S        28      43.857142857142854      00:44
Mithun S        29      44.142857142857146      00:44
Mithun S        30      38.833333333333336      00:39
Mukesh  26      0.0     00:00
Mukesh  27      0.0     00:00
Mukesh  28      0.0     00:00
Mukesh  29      0.0     00:00
Mukesh  30      16.666666666666668      00:17
Mukesh Rao      26      0.0     00:00
Mukesh Rao      27      56.285714285714285      00:56
Mukesh Rao      28      0.0     00:00
Mukesh Rao      29      0.0     00:00
Mukesh Rao      30      0.0     00:00
Muskan Garg     26      0.0     00:00
Muskan Garg     27      0.0     00:00
Muskan Garg     28      0.0     00:00
Muskan Garg     29      0.0     00:00
Muskan Garg     30      29.666666666666668      00:30
Nandani Gupta   26      108.33333333333333      01:48
Nandani Gupta   27      57.857142857142854      00:58
Nandani Gupta   28      50.714285714285715      00:51
Nandani Gupta   29      57.57142857142857       00:58
Nandani Gupta   30      51.333333333333336      00:51
Nishtha Jain    26      0.0     00:00
Nishtha Jain    27      72.28571428571429       01:12
Nishtha Jain    28      60.714285714285715      01:01
Nishtha Jain    29      88.57142857142857       01:29
Nishtha Jain    30      45.5    00:46
Nitin M 26      0.0     00:00
Nitin M 27      0.0     00:00
Nitin M 28      0.0     00:00
Nitin M 29      0.0     00:00
Nitin M 30      0.0     00:00
Prabir Kumar Satapathy  26      0.0     00:00
Prabir Kumar Satapathy  27      60.857142857142854      01:01
Prabir Kumar Satapathy  28      34.0    00:34
Prabir Kumar Satapathy  29      39.857142857142854      00:40
Prabir Kumar Satapathy  30      32.833333333333336      00:33
Prateek _iot    26      0.0     00:00
Prateek _iot    27      9.428571428571429       00:09
Prateek _iot    28      21.857142857142858      00:22
Prateek _iot    29      32.714285714285715      00:33
Prateek _iot    30      37.833333333333336      00:38
Prerna Singh    26      64.66666666666667       01:05
Prerna Singh    27      49.285714285714285      00:49
Prerna Singh    28      41.857142857142854      00:42
Prerna Singh    29      43.42857142857143       00:43
Prerna Singh    30      49.0    00:49
Rishav Dash     26      45.0    00:45
Rishav Dash     27      30.071428571428573      00:30
Rishav Dash     28      23.857142857142858      00:24
Rishav Dash     29      25.928571428571427      00:26
Rishav Dash     30      35.916666666666664      00:36
Rohan   26      0.0     00:00
Rohan   27      0.0     00:00
Rohan   28      0.0     00:00
Rohan   29      0.0     00:00
Rohan   30      0.0     00:00
Saif Khan       26      0.0     00:00
Saif Khan       27      0.0     00:00
Saif Khan       28      0.0     00:00
Saif Khan       29      0.0     00:00
Saif Khan       30      0.0     00:00
Saikumarreddy N 26      0.0     00:00
Saikumarreddy N 27      0.0     00:00
Saikumarreddy N 28      22.142857142857142      00:22
Saikumarreddy N 29      51.0    00:51
Saikumarreddy N 30      40.5    00:41
Samprit         26      0.0     00:00
Samprit         27      0.0     00:00
Samprit         28      0.0     00:00
Samprit         29      0.0     00:00
Samprit         30      0.0     00:00
Sandipan Saha   26      0.0     00:00
Sandipan Saha   27      0.0     00:00
Sandipan Saha   28      25.285714285714285      00:25
Sandipan Saha   29      0.0     00:00
Sandipan Saha   30      0.0     00:00
Sanjeev Kumar   26      76.0    01:16
Sanjeev Kumar   27      39.0    00:39
Sanjeev Kumar   28      70.14285714285714       01:10
Sanjeev Kumar   29      41.142857142857146      00:41
Sanjeev Kumar   30      42.666666666666664      00:43
Sanjeevan       26      0.0     00:00
Sanjeevan       27      0.0     00:00
Sanjeevan       28      0.0     00:00
Sanjeevan       29      0.0     00:00
Sanjeevan       30      0.0     00:00
Saurabh Shukla  26      7.0     00:07
Saurabh Shukla  27      10.0    00:10
Saurabh Shukla  28      2.0     00:02
Saurabh Shukla  29      0.0     00:00
Saurabh Shukla  30      0.0     00:00
Shiva Srivastava        26      0.0     00:00
Shiva Srivastava        27      0.0     00:00
Shiva Srivastava        28      0.0     00:00
Shiva Srivastava        29      23.857142857142858      00:24
Shiva Srivastava        30      22.166666666666668      00:22
Shivan K        26      56.0    00:56
Shivan K        27      52.857142857142854      00:53
Shivan K        28      29.0    00:29
Shivan K        29      67.28571428571429       01:07
Shivan K        30      37.5    00:38
Shivan_S        26      0.0     00:00
Shivan_S        27      10.428571428571429      00:10
Shivan_S        28      0.0     00:00
Shivan_S        29      0.0     00:00
Shivan_S        30      0.0     00:00
Shivananda Sonwane      26      77.0    01:17
Shivananda Sonwane      27      52.57142857142857       00:53
Shivananda Sonwane      28      49.142857142857146      00:49
Shivananda Sonwane      29      59.57142857142857       01:00
Shivananda Sonwane      30      53.333333333333336      00:53
Shubham Sharma  26      54.333333333333336      00:54
Shubham Sharma  27      41.57142857142857       00:42
Shubham Sharma  28      36.857142857142854      00:37
Shubham Sharma  29      51.142857142857146      00:51
Shubham Sharma  30      63.333333333333336      01:03
Sowmiya Sivakumar       26      0.0     00:00
Sowmiya Sivakumar       27      0.0     00:00
Sowmiya Sivakumar       28      0.0     00:00
Sowmiya Sivakumar       29      60.42857142857143       01:00
Sowmiya Sivakumar       30      60.333333333333336      01:00
Spuri   26      0.0     00:00
Spuri   27      0.0     00:00
Spuri   28      0.0     00:00
Spuri   29      0.0     00:00
Spuri   30      0.0     00:00
Sudhanshu Kumar 26      0.0     00:00
Sudhanshu Kumar 27      6.857142857142857       00:07
Sudhanshu Kumar 28      10.285714285714286      00:10
Sudhanshu Kumar 29      0.0     00:00
Sudhanshu Kumar 30      0.0     00:00
Suraj S Bilgi   26      0.0     00:00
Suraj S Bilgi   27      0.0     00:00
Suraj S Bilgi   28      0.0     00:00
Suraj S Bilgi   29      0.0     00:00
Suraj S Bilgi   30      30.333333333333332      00:30
Swati   26      107.0   01:47
Swati   27      69.57142857142857       01:10
Swati   28      77.14285714285714       01:17
Swati   29      30.285714285714285      00:30
Swati   30      29.0    00:29
Tarun   26      0.0     00:00
Tarun   27      0.0     00:00
Tarun   28      0.0     00:00
Tarun   29      0.0     00:00
Tarun   30      0.0     00:00
Uday Mishra     26      0.0     00:00
Uday Mishra     27      0.0     00:00
Uday Mishra     28      0.0     00:00
Uday Mishra     29      0.0     00:00
Uday Mishra     30      0.0     00:00
Vasanth P       26      0.0     00:00
Vasanth P       27      0.0     00:00
Vasanth P       28      0.0     00:00
Vasanth P       29      0.0     00:00
Vasanth P       30      0.0     00:00
Vivek   26      0.0     00:00
Vivek   27      15.285714285714286      00:15
Vivek   28      43.42857142857143       00:43
Vivek   29      0.0     00:00
Vivek   30      0.0     00:00
Wasim   26      0.0     00:00
Wasim   27      8.0     00:08
Wasim   28      29.857142857142858      00:30
Wasim   29      46.142857142857146      00:46
Wasim   30      50.5    00:51
Zeeshan         26      0.0     00:00
Zeeshan         27      23.571428571428573      00:24
Zeeshan         28      123.42857142857143      02:03
Zeeshan         29      62.857142857142854      01:03
Zeeshan         30      63.833333333333336      01:04
Time taken: 34.294 seconds, Fetched: 350 row(s)

## 13. average weekly resolution time for each agents 

hive> with cte as (select weekofyear(cast(to_date(from_unixtime(unix_timestamp(dat,'MM/dd/yyyy'))) as date)) as wk,agent,unix_timestamp(avg_resol_time,'hh:mm:ss')-28800 as result from agent_performance)
    > select agent,wk,avg(result),from_unixtime(cast(round(avg(result),0) as int)-14400,'mm:ss') as pp from cte group by agent,wk;
Query ID = cloudera_20220921145353_a9ed78ff-76eb-4b0d-99e7-2d5c43f1d196
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663066990219_0091, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663066990219_0091/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663066990219_0091
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-21 14:53:36,468 Stage-1 map = 0%,  reduce = 0%
2022-09-21 14:53:47,301 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.15 sec
2022-09-21 14:53:59,331 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 5.73 sec
MapReduce Total cumulative CPU time: 5 seconds 730 msec
Ended Job = job_1663066990219_0091
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 5.73 sec   HDFS Read: 121928 HDFS Write: 10895 SUCCESS
Total MapReduce CPU Time Spent: 5 seconds 730 msec
OK
agent   wk      _c2     pp
Abhishek        26      0.0     00:00
Abhishek        27      0.0     00:00
Abhishek        28      0.0     00:00
Abhishek        29      0.0     00:00
Abhishek        30      0.0     00:00
Aditya  26      0.0     00:00
Aditya  27      0.0     00:00
Aditya  28      0.0     00:00
Aditya  29      0.0     00:00
Aditya  30      0.0     00:00
Aditya Shinde   26      927.0   15:27
Aditya Shinde   27      903.8571428571429       15:04
Aditya Shinde   28      1358.7142857142858      22:39
Aditya Shinde   29      0.0     00:00
Aditya Shinde   30      0.0     00:00
Aditya_iot      26      0.0     00:00
Aditya_iot      27      309.85714285714283      05:10
Aditya_iot      28      911.0   15:11
Aditya_iot      29      672.1428571428571       11:12
Aditya_iot      30      738.0   12:18
Amersh  26      0.0     00:00
Amersh  27      0.0     00:00
Amersh  28      0.0     00:00
Amersh  29      0.0     00:00
Amersh  30      0.0     00:00
Ameya Jain      26      0.0     00:00
Ameya Jain      27      15.285714285714286      00:15
Ameya Jain      28      483.7142857142857       08:04
Ameya Jain      29      497.85714285714283      08:18
Ameya Jain      30      479.5   08:00
Anirudh         26      0.0     00:00
Anirudh         27      487.85714285714283      08:08
Anirudh         28      307.0   05:07
Anirudh         29      0.0     00:00
Anirudh         30      0.0     00:00
Ankit Sharma    26      0.0     00:00
Ankit Sharma    27      0.0     00:00
Ankit Sharma    28      0.0     00:00
Ankit Sharma    29      0.0     00:00
Ankit Sharma    30      0.0     00:00
Ankitjha        26      0.0     00:00
Ankitjha        27      183.14285714285714      03:03
Ankitjha        28      0.0     00:00
Ankitjha        29      0.0     00:00
Ankitjha        30      58.666666666666664      00:59
Anurag Tiwari   26      0.0     00:00
Anurag Tiwari   27      316.85714285714283      05:17
Anurag Tiwari   28      0.0     00:00
Anurag Tiwari   29      0.0     00:00
Anurag Tiwari   30      0.0     00:00
Aravind         26      0.0     00:00
Aravind         27      622.2857142857143       10:22
Aravind         28      865.4285714285714       14:25
Aravind         29      807.5714285714286       13:28
Aravind         30      0.0     00:00
Ashad Nasim     26      0.0     00:00
Ashad Nasim     27      89.71428571428571       01:30
Ashad Nasim     28      0.0     00:00
Ashad Nasim     29      0.0     00:00
Ashad Nasim     30      0.0     00:00
Ashish  26      0.0     00:00
Ashish  27      0.0     00:00
Ashish  28      0.0     00:00
Ashish  29      0.0     00:00
Ashish  30      0.0     00:00
Ayushi Mishra   26      1021.6666666666666      17:02
Ayushi Mishra   27      743.0   12:23
Ayushi Mishra   28      876.7142857142857       14:37
Ayushi Mishra   29      1107.4285714285713      18:27
Ayushi Mishra   30      920.5   15:21
Bharath         26      388.3333333333333       06:28
Bharath         27      741.7142857142857       12:22
Bharath         28      441.57142857142856      07:22
Bharath         29      564.7142857142857       09:25
Bharath         30      1004.5  16:45
Boktiar Ahmed Bappy     26      900.3333333333334       15:00
Boktiar Ahmed Bappy     27      1988.7142857142858      33:09
Boktiar Ahmed Bappy     28      408.14285714285717      06:48
Boktiar Ahmed Bappy     29      695.7142857142857       11:36
Boktiar Ahmed Bappy     30      1061.0  17:41
Chaitra K Hiremath      26      0.0     00:00
Chaitra K Hiremath      27      0.0     00:00
Chaitra K Hiremath      28      0.0     00:00
Chaitra K Hiremath      29      42.714285714285715      00:43
Chaitra K Hiremath      30      395.0   06:35
Deepranjan Gupta        26      1208.0  20:08
Deepranjan Gupta        27      1548.2857142857142      25:48
Deepranjan Gupta        28      992.0   16:32
Deepranjan Gupta        29      1320.0  22:00
Deepranjan Gupta        30      1148.1666666666667      19:08
Dibyanshu       26      0.0     00:00
Dibyanshu       27      105.71428571428571      01:46
Dibyanshu       28      0.0     00:00
Dibyanshu       29      0.0     00:00
Dibyanshu       30      0.0     00:00
Harikrishnan Shaji      26      0.0     00:00
Harikrishnan Shaji      27      359.7142857142857       06:00
Harikrishnan Shaji      28      762.2857142857143       12:42
Harikrishnan Shaji      29      1070.7142857142858      17:51
Harikrishnan Shaji      30      834.0   13:54
Hitesh Choudhary        26      0.0     00:00
Hitesh Choudhary        27      0.0     00:00
Hitesh Choudhary        28      12.142857142857142      00:12
Hitesh Choudhary        29      0.0     00:00
Hitesh Choudhary        30      0.0     00:00
Hrisikesh Neogi 26      805.3333333333334       13:25
Hrisikesh Neogi 27      853.2857142857143       14:13
Hrisikesh Neogi 28      747.1428571428571       12:27
Hrisikesh Neogi 29      1130.4285714285713      18:50
Hrisikesh Neogi 30      1042.8333333333333      17:23
Hyder Abbas     26      0.0     00:00
Hyder Abbas     27      0.0     00:00
Hyder Abbas     28      0.0     00:00
Hyder Abbas     29      0.0     00:00
Hyder Abbas     30      0.0     00:00
Ineuron Intelligence    26      0.0     00:00
Ineuron Intelligence    27      0.0     00:00
Ineuron Intelligence    28      0.0     00:00
Ineuron Intelligence    29      0.0     00:00
Ineuron Intelligence    30      0.0     00:00
Ishawant Kumar  26      0.0     00:00
Ishawant Kumar  27      851.8571428571429       14:12
Ishawant Kumar  28      794.7142857142857       13:15
Ishawant Kumar  29      1079.4285714285713      17:59
Ishawant Kumar  30      1145.1666666666667      19:05
Jawala Prakash  26      657.0   10:57
Jawala Prakash  27      477.57142857142856      07:58
Jawala Prakash  28      874.5714285714286       14:35
Jawala Prakash  29      898.8571428571429       14:59
Jawala Prakash  30      947.5   15:48
Jayant Kumar    26      763.6666666666666       12:44
Jayant Kumar    27      840.1428571428571       14:00
Jayant Kumar    28      96.57142857142857       01:37
Jayant Kumar    29      0.0     00:00
Jayant Kumar    30      0.0     00:00
Jaydeep Dixit   26      786.0   13:06
Jaydeep Dixit   27      1090.2857142857142      18:10
Jaydeep Dixit   28      1151.0  19:11
Jaydeep Dixit   29      1282.0  21:22
Jaydeep Dixit   30      901.8333333333334       15:02
Khushboo Priya  26      1106.0  18:26
Khushboo Priya  27      1435.5714285714287      23:56
Khushboo Priya  28      954.0   15:54
Khushboo Priya  29      755.0   12:35
Khushboo Priya  30      800.5   13:21
Madhulika G     26      775.0   12:55
Madhulika G     27      873.8571428571429       14:34
Madhulika G     28      987.7142857142857       16:28
Madhulika G     29      753.7142857142857       12:34
Madhulika G     30      1224.1666666666667      20:24
Mahak   26      0.0     00:00
Mahak   27      172.0   02:52
Mahak   28      0.0     00:00
Mahak   29      0.0     00:00
Mahak   30      0.0     00:00
Mahesh Sarade   26      0.0     00:00
Mahesh Sarade   27      362.57142857142856      06:03
Mahesh Sarade   28      615.8571428571429       10:16
Mahesh Sarade   29      786.2857142857143       13:06
Mahesh Sarade   30      688.6666666666666       11:29
Maitry  26      659.3333333333334       10:59
Maitry  27      679.5714285714286       11:20
Maitry  28      837.4285714285714       13:57
Maitry  29      1019.0  16:59
Maitry  30      547.0   09:07
Maneesh         26      0.0     00:00
Maneesh         27      178.57142857142858      02:59
Maneesh         28      0.0     00:00
Maneesh         29      0.0     00:00
Maneesh         30      0.0     00:00
Manjunatha A    26      1070.0  17:50
Manjunatha A    27      1205.142857142857       20:05
Manjunatha A    28      969.1428571428571       16:09
Manjunatha A    29      655.1428571428571       10:55
Manjunatha A    30      1251.6666666666667      20:52
Mithun S        26      0.0     00:00
Mithun S        27      209.14285714285714      03:29
Mithun S        28      347.42857142857144      05:47
Mithun S        29      456.0   07:36
Mithun S        30      552.0   09:12
Mukesh  26      0.0     00:00
Mukesh  27      0.0     00:00
Mukesh  28      0.0     00:00
Mukesh  29      0.0     00:00
Mukesh  30      379.6666666666667       06:20
Mukesh Rao      26      0.0     00:00
Mukesh Rao      27      1973.857142857143       32:54
Mukesh Rao      28      0.0     00:00
Mukesh Rao      29      0.0     00:00
Mukesh Rao      30      0.0     00:00
Muskan Garg     26      0.0     00:00
Muskan Garg     27      0.0     00:00
Muskan Garg     28      0.0     00:00
Muskan Garg     29      0.0     00:00
Muskan Garg     30      576.0   09:36
Nandani Gupta   26      1050.3333333333333      17:30
Nandani Gupta   27      1002.0  16:42
Nandani Gupta   28      1021.7142857142857      17:02
Nandani Gupta   29      1111.857142857143       18:32
Nandani Gupta   30      1265.1666666666667      21:05
Nishtha Jain    26      0.0     00:00
Nishtha Jain    27      712.2857142857143       11:52
Nishtha Jain    28      658.0   10:58
Nishtha Jain    29      516.5714285714286       08:37
Nishtha Jain    30      590.6666666666666       09:51
Nitin M 26      0.0     00:00
Nitin M 27      0.0     00:00
Nitin M 28      0.0     00:00
Nitin M 29      0.0     00:00
Nitin M 30      0.0     00:00
Prabir Kumar Satapathy  26      0.0     00:00
Prabir Kumar Satapathy  27      314.0   05:14
Prabir Kumar Satapathy  28      500.42857142857144      08:20
Prabir Kumar Satapathy  29      459.57142857142856      07:40
Prabir Kumar Satapathy  30      296.8333333333333       04:57
Prateek _iot    26      0.0     00:00
Prateek _iot    27      317.0   05:17
Prateek _iot    28      690.8571428571429       11:31
Prateek _iot    29      573.7142857142857       09:34
Prateek _iot    30      586.0   09:46
Prerna Singh    26      618.6666666666666       10:19
Prerna Singh    27      821.7142857142857       13:42
Prerna Singh    28      1405.7142857142858      23:26
Prerna Singh    29      883.5714285714286       14:44
Prerna Singh    30      1018.1666666666666      16:58
Rishav Dash     26      569.6666666666666       09:30
Rishav Dash     27      465.14285714285717      07:45
Rishav Dash     28      403.57142857142856      06:44
Rishav Dash     29      602.7857142857143       10:03
Rishav Dash     30      546.1666666666666       09:06
Rohan   26      0.0     00:00
Rohan   27      0.0     00:00
Rohan   28      0.0     00:00
Rohan   29      0.0     00:00
Rohan   30      0.0     00:00
Saif Khan       26      0.0     00:00
Saif Khan       27      0.0     00:00
Saif Khan       28      0.0     00:00
Saif Khan       29      0.0     00:00
Saif Khan       30      0.0     00:00
Saikumarreddy N 26      0.0     00:00
Saikumarreddy N 27      0.0     00:00
Saikumarreddy N 28      506.14285714285717      08:26
Saikumarreddy N 29      573.4285714285714       09:33
Saikumarreddy N 30      607.3333333333334       10:07
Samprit         26      0.0     00:00
Samprit         27      14.714285714285714      00:15
Samprit         28      0.0     00:00
Samprit         29      0.0     00:00
Samprit         30      0.0     00:00
Sandipan Saha   26      0.0     00:00
Sandipan Saha   27      0.0     00:00
Sandipan Saha   28      676.4285714285714       11:16
Sandipan Saha   29      0.0     00:00
Sandipan Saha   30      0.0     00:00
Sanjeev Kumar   26      1240.0  20:40
Sanjeev Kumar   27      902.1428571428571       15:02
Sanjeev Kumar   28      1167.5714285714287      19:28
Sanjeev Kumar   29      838.2857142857143       13:58
Sanjeev Kumar   30      1145.5  19:06
Sanjeevan       26      0.0     00:00
Sanjeevan       27      0.0     00:00
Sanjeevan       28      0.0     00:00
Sanjeevan       29      0.0     00:00
Sanjeevan       30      0.0     00:00
Saurabh Shukla  26      211.66666666666666      03:32
Saurabh Shukla  27      157.85714285714286      02:38
Saurabh Shukla  28      57.42857142857143       00:57
Saurabh Shukla  29      0.0     00:00
Saurabh Shukla  30      0.0     00:00
Shiva Srivastava        26      0.0     00:00
Shiva Srivastava        27      0.0     00:00
Shiva Srivastava        28      0.0     00:00
Shiva Srivastava        29      108.0   01:48
Shiva Srivastava        30      318.0   05:18
Shivan K        26      1835.0  30:35
Shivan K        27      1158.4285714285713      19:18
Shivan K        28      707.0   11:47
Shivan K        29      806.1428571428571       13:26
Shivan K        30      723.6666666666666       12:04
Shivan_S        26      0.0     00:00
Shivan_S        27      157.42857142857142      02:37
Shivan_S        28      0.0     00:00
Shivan_S        29      0.0     00:00
Shivan_S        30      0.0     00:00
Shivananda Sonwane      26      1334.0  22:14
Shivananda Sonwane      27      1253.5714285714287      20:54
Shivananda Sonwane      28      1170.142857142857       19:30
Shivananda Sonwane      29      1230.2857142857142      20:30
Shivananda Sonwane      30      1413.0  23:33
Shubham Sharma  26      984.3333333333334       16:24
Shubham Sharma  27      1178.4285714285713      19:38
Shubham Sharma  28      1019.1428571428571      16:59
Shubham Sharma  29      923.2857142857143       15:23
Shubham Sharma  30      1082.6666666666667      18:03
Sowmiya Sivakumar       26      0.0     00:00
Sowmiya Sivakumar       27      0.0     00:00
Sowmiya Sivakumar       28      0.0     00:00
Sowmiya Sivakumar       29      674.7142857142857       11:15
Sowmiya Sivakumar       30      999.5   16:40
Spuri   26      0.0     00:00
Spuri   27      0.0     00:00
Spuri   28      0.0     00:00
Spuri   29      0.0     00:00
Spuri   30      0.0     00:00
Sudhanshu Kumar 26      0.0     00:00
Sudhanshu Kumar 27      152.85714285714286      02:33
Sudhanshu Kumar 28      348.42857142857144      05:48
Sudhanshu Kumar 29      0.0     00:00
Sudhanshu Kumar 30      0.0     00:00
Suraj S Bilgi   26      0.0     00:00
Suraj S Bilgi   27      0.0     00:00
Suraj S Bilgi   28      0.0     00:00
Suraj S Bilgi   29      0.0     00:00
Suraj S Bilgi   30      788.3333333333334       13:08
Swati   26      1059.3333333333333      17:39
Swati   27      883.2857142857143       14:43
Swati   28      1031.7142857142858      17:12
Swati   29      469.0   07:49
Swati   30      371.8333333333333       06:12
Tarun   26      904.6666666666666       15:05
Tarun   27      0.0     00:00
Tarun   28      0.0     00:00
Tarun   29      0.0     00:00
Tarun   30      0.0     00:00
Uday Mishra     26      0.0     00:00
Uday Mishra     27      0.0     00:00
Uday Mishra     28      0.0     00:00
Uday Mishra     29      0.0     00:00
Uday Mishra     30      0.0     00:00
Vasanth P       26      0.0     00:00
Vasanth P       27      0.0     00:00
Vasanth P       28      0.0     00:00
Vasanth P       29      0.0     00:00
Vasanth P       30      0.0     00:00
Vivek   26      0.0     00:00
Vivek   27      145.0   02:25
Vivek   28      509.7142857142857       08:30
Vivek   29      0.0     00:00
Vivek   30      0.0     00:00
Wasim   26      0.0     00:00
Wasim   27      196.14285714285714      03:16
Wasim   28      867.0   14:27
Wasim   29      985.0   16:25
Wasim   30      1054.8333333333333      17:35
Zeeshan         26      0.0     00:00
Zeeshan         27      323.0   05:23
Zeeshan         28      984.2857142857143       16:24
Zeeshan         29      794.2857142857143       13:14
Zeeshan         30      773.5   12:54
Time taken: 34.605 seconds, Fetched: 350 row(s)

##14. Find the number of chat on which they have received a feedback 

>>  select  agent, sum(tot_feedback) as chat_with_feedback from agent_performance where tot_feedback<>0 group by agent;
Query ID = cloudera_20220922093232_73d95cf7-6c52-43e0-ad89-89e43458b21c
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663861700296_0001, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663861700296_0001/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663861700296_0001
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-22 09:32:59,001 Stage-1 map = 0%,  reduce = 0%
2022-09-22 09:33:15,912 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 6.32 sec
2022-09-22 09:33:31,249 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.76 sec
MapReduce Total cumulative CPU time: 11 seconds 760 msec
Ended Job = job_1663861700296_0001
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.76 sec   HDFS Read: 118669 HDFS Write: 867 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 760 msec
OK
agent   chat_with_feedback
Aditya Shinde   153
Aditya_iot      131
Ameya Jain      228
Anirudh         39
Ankitjha        3
Anurag Tiwari   3
Aravind         233
Ashad Nasim     9
Ayushi Mishra   329
Bharath         247
Boktiar Ahmed Bappy     311
Chaitra K Hiremath      37
Deepranjan Gupta        312
Harikrishnan Shaji      231
Hrisikesh Neogi 367
Ishawant Kumar  202
Jawala Prakash  250
Jayant Kumar    70
Jaydeep Dixit   305
Khushboo Priya  289
Madhulika G     281
Mahak   5
Mahesh Sarade   216
Maitry  347
Maneesh         3
Manjunatha A    254
Mithun S        364
Mukesh  17
Mukesh Rao      5
Muskan Garg     37
Nandani Gupta   308
Nishtha Jain    257
Prabir Kumar Satapathy  222
Prateek _iot    107
Prerna Singh    235
Rishav Dash     264
Saikumarreddy N 290
Sandipan Saha   18
Sanjeev Kumar   311
Saurabh Shukla  8
Shiva Srivastava        46
Shivan K        243
Shivan_S        4
Shivananda Sonwane      263
Shubham Sharma  300
Sowmiya Sivakumar       141
Sudhanshu Kumar 2
Suraj S Bilgi   15
Swati   302
Tarun   6
Vivek   20
Wasim   284
Zeeshan         335
Time taken: 60.536 seconds, Fetched: 53 row(s)

## 15. Total contribution hour for each and every agents weekly basis 

>>  with cte as (select weekofyear(cast(to_date(from_unixtime(unix_timestamp(dat,'dd-MMM-yy'))) as date)) as wk,agent,unix_timestamp(duration,'hh:mm:ss')-28800 as result from agent_login_report) select agent,wk, from_unixtime(cast(round(avg(result),0) as int)-14400,'hh:mm:ss') as avg_hrs from cte group by agent,wk;
Query ID = cloudera_20220922105353_8f2d7b58-227b-45c1-b2a6-9d525ad577a2
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1663861700296_0008, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663861700296_0008/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663861700296_0008
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-09-22 10:53:42,237 Stage-1 map = 0%,  reduce = 0%
2022-09-22 10:53:59,348 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.32 sec
2022-09-22 10:54:17,946 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 15.01 sec
MapReduce Total cumulative CPU time: 15 seconds 10 msec
Ended Job = job_1663861700296_0008
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 15.01 sec   HDFS Read: 67083 HDFS Write: 2243 SUCCESS
Total MapReduce CPU Time Spent: 15 seconds 10 msec
OK
agent   wk      avg_hrs
Aditya Shinde   30      12:02:10
Aditya_iot      29      02:01:54
Aditya_iot      30      01:36:22
Amersh  30      12:45:58
Ameya Jain      29      03:26:26
Ameya Jain      30      05:59:51
Ankitjha        30      12:34:00
Anurag Tiwari   29      12:03:58
Anurag Tiwari   30      12:04:34
Aravind 29      03:01:46
Aravind 30      12:01:55
Ayushi Mishra   29      03:33:29
Ayushi Mishra   30      01:33:50
Bharath 29      04:48:51
Bharath 30      06:00:05
Boktiar Ahmed Bappy     29      03:33:00
Boktiar Ahmed Bappy     30      01:52:36
Chaitra K Hiremath      29      01:07:03
Chaitra K Hiremath      30      02:55:02
Deepranjan Gupta        29      01:53:04
Deepranjan Gupta        30      01:47:24
Dibyanshu       29      12:16:29
Dibyanshu       30      12:13:56
Harikrishnan Shaji      29      02:22:55
Harikrishnan Shaji      30      02:18:20
Hrisikesh Neogi 29      01:55:15
Hrisikesh Neogi 30      01:20:02
Hyder Abbas     29      12:20:10
Hyder Abbas     30      12:03:07
Ineuron Intelligence    29      01:26:55
Ishawant Kumar  29      01:21:13
Ishawant Kumar  30      12:52:07
Jawala Prakash  29      04:03:24
Jawala Prakash  30      02:12:22
Jaydeep Dixit   29      06:59:09
Jaydeep Dixit   30      03:35:07
Khushboo Priya  29      02:42:52
Khushboo Priya  30      02:11:03
Madhulika G     29      02:35:06
Madhulika G     30      02:53:13
Mahesh Sarade   29      02:32:54
Mahesh Sarade   30      12:40:15
Maitry  29      06:09:52
Maitry  30      06:17:14
Manjunatha A    29      06:07:01
Manjunatha A    30      04:35:05
Mithun S        29      02:53:48
Mithun S        30      03:28:27
Mukesh  30      02:58:06
Muskan Garg     29      12:49:47
Muskan Garg     30      01:45:08
Nandani Gupta   29      04:20:01
Nandani Gupta   30      03:15:45
Nishtha Jain    29      02:45:52
Nishtha Jain    30      02:10:25
Nitin M 29      12:47:56
Prabir Kumar Satapathy  29      01:15:06
Prabir Kumar Satapathy  30      01:19:16
Prateek _iot    29      12:48:28
Prateek _iot    30      01:23:37
Prerna Singh    29      03:05:11
Prerna Singh    30      02:15:59
Rishav Dash     29      02:41:57
Rishav Dash     30      04:34:35
Saikumarreddy N 29      04:09:48
Saikumarreddy N 30      04:32:21
Sanjeev Kumar   29      01:45:36
Sanjeev Kumar   30      02:48:51
Saurabh Shukla  29      12:25:00
Shiva Srivastava        29      12:22:52
Shiva Srivastava        30      01:18:32
Shivan K        29      01:23:34
Shivan K        30      12:48:28
Shivananda Sonwane      29      04:10:01
Shivananda Sonwane      30      02:50:43
Shubham Sharma  29      01:23:13
Shubham Sharma  30      01:47:29
Sowmiya Sivakumar       29      02:26:17
Sowmiya Sivakumar       30      01:37:43
Sudhanshu Kumar 29      03:03:25
Sudhanshu Kumar 30      07:15:32
Suraj S Bilgi   30      02:30:43
Swati   29      04:42:53
Swati   30      06:08:33
Tarun   26      10:08:20
Wasim   29      02:10:50
Wasim   30      02:35:45
Zeeshan 29      04:04:17
Zeeshan 30      06:09:55
Time taken: 54.299 seconds, Fetched: 89 row(s)

## 16. Perform inner join, left join and right join based on the agent column and after joining the table export that data into your local system.
>> [cloudera@quickstart data]$ hive -e 'use hive_database_project1; select * from agent_login_report al left join agent_performance ap on al.agent = ap.agent' | sed 's/[\t]/,/g' > /home/cloudera/data/agent_leftjoin.csv
>> [cloudera@quickstart data]$ hive -e 'use hive_database_project1; select * from agent_login_report al right join agent_performance ap on al.agent = ap.agent' | sed 's/[\t]/,/g' > /home/cloudera/data/agent_rightjoin.csv
>> [cloudera@quickstart data]$ hive -e 'use hive_database_project1; select * from agent_login_report al join agent_performance ap on al.agent = ap.agent' | sed 's/[\t]/,/g' > /home/cloudera/data/agent_join.csv

## 17. Perform partitioning on top of the agent column and then on top of that perform bucketing for each partitioning.
## agent_login_report:
Partitioning:
>> create table agent_login_report_partition(slid int, dat string, login string, logout string, duration string) partitioned by (agent string);
>> insert overwrite agent_login_report_partition partition(agent) select slid,agent, dat, login, logout , duration) from agent_login_report;

## Bucketing:
>> set hive.enforce.bucketing=true;
>> create table agent_login_report_bucket(slid int,agent string, dat string, login string, logout string, duration string) clustered by (agent) sorted by(agent) into 49 buckets;
>> insert overwrite agent_login_report_bucket select * from agent_login_report_partition;

## agent_performance
Partitioning:
>> create table agent_performance_partition(slid int,dat string, tot_chat int, avg_rsp_time string, avg_resol_time string, avg_rating double,tot_feedback int) partitioned by (agent string);
>> insert overwrite agent_performance_partition partition(agent) select * from agent_performance;

Bucketing:
>> set hive.enforce.bucketing=true;
>> create table agent_performance_bucket(slid int,dat string,agent string, tot_chat int, avg_rsp_time string, avg_resol_time string, avg_rating double,tot_feedback int) clustered by (agent) sorted by(agent) into 70 buckets;
>> insert overwrite agent_performance_bucket select * from agent_performance_partition;
