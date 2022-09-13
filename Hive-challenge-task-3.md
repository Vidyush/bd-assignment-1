### Q1. Will the reducer work or not if you use “Limit 1” in any HiveQL query?

A1. The reducer will not work when limit is invoked for a normal read query. But if the limit is used with aggregate functions or joins or group by or order by then they will require reducer but limit still will not require reducer.
------------------------------------------------------------------->

### Q2. Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. Then, what will happen if we have multiple clients trying to access Hive at the same time?

A2. The default metastore configuration is stored in derby. And if multiple clients will try to access it at the same time it will not accept any incoming request as one one connection is entertained at one time. That is one of the drawback for using default metastore configuration and one should use oracle or postgress for instead of derby for the same. And even if you have to install hive again you metastore data will be lost, so having an external metastore is very beneficial(RDBMS).
------------------------------------------------------------------->

### Q3. Suppose, I create a table that contains details of all the transactions done by the customers: CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
### Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. How will you solve this problem and list the steps that I will be taking in order to do so?
