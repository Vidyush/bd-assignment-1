# creating table and loading data:

hive> create table parking_csv( sumnum int, plateid string, regstate string, placetype string, issuedate string, violcode int, vehbody string, vehmake string, issueag string, stcode1 int, stcode2 int, stcode3 int, vehexp int, violloc string, violprec int, issuerprec int, issuercode int, issuercmd string, issuersqd string, violtime string, timefo string, violcnt string, vioinfrop string, housenum string, stname string, intst string, datefo int, lawsec int, subdiv string, viollegalcode string, daysparkineff string, frmhrsineff string, tohrsineff string, vehclr string, unregveh string, vehyear int, metnum string, feetfrmcurb int, violpostcode string,violdesc string, nostandstopviol string, hydviol string, dblparkviol string) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");

hive> load data local inpath 'file:///home/cloudera/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv' into table parking_csv;

Part 1:
1) select count(sumnum) as tickets from parking_csv where year(cast(to_date(from_unixtime(unix_timestamp(issuedate,'dd/MM/yyyy'))) as date))= 2017;

2) 
## Partitioning

# parking_csv_partion_year
hive> create table parking_csv_partion_year( sumnum int, plateid string, regstate string, placetype string, issuedate string, violcode int, vehbody string, vehmake string, issueag string, stcode1 int, stcode2 int, stcode3 int, vehexp int, violloc string, violprec int, issuerprec int, issuercode int, issuercmd string, issuersqd string, violtime string, timefo string, violcnt string, vioinfrop string, housenum string, stname string, intst string, datefo int, lawsec int, subdiv string, viollegalcode string, daysparkineff string, frmhrsineff string, tohrsineff string, vehclr string, unregveh string, vehyear int, metnum string, feetfrmcurb int, violpostcode string,violdesc string, nostandstopviol string, hydviol string, dblparkviol string) partitioned by (issue_year int);
OK
Time taken: 0.31 seconds

hive> insert overwrite table parking_csv_partion_year partition(issue_year = 2017) select sumnum , plateid , regstate , placetype , issuedate , violcode , vehbody , vehmake , issueag , stcode1 , stcode2 , stcode3 , vehexp , violloc , violprec , issuerprec , issuercode , issuercmd , issuersqd , violtime , timefo , violcnt , vioinfrop , housenum , stname , intst , datefo , lawsec , subdiv , viollegalcode , daysparkineff , frmhrsineff , tohrsineff , vehclr , unregveh , vehyear , metnum , feetfrmcurb , violpostcode ,violdesc , nostandstopviol , hydviol , dblparkviol  from parking_csv_bkp2 where issue_year =2017;

# fetching data frm partition:
 >>select * from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 limit 5;

3.) Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are(i.e. tickets where either "Street Code 1" or "Street Code 2" or "Street Code 3" is empty )
>> select count(sumnum) as ticketswithoutaddress from parking_csv_partion_year where stcode1=0 or stcode2=0 or stcode3=0;

Part2:

1.) How often does each violation code occur? (frequency of violation codes - find the top 5)
>> select violcode, count(violcode) as violationcode_frequency from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by violcode  order by violationcode_frequency desc limit 5;

2.) How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
>> select vehbody, count(sumnum) as no_of_tickets from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by vehbody order by no_of_tickets desc limit 5;
>> select vehmake, count(sumnum) as no_of_tickets from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by vehmake order by no_of_tickets desc limit 5;

# 3.) A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
 # a.) Violating Precincts (this is the precinct of the zone where the violation occurred)
 # b.) Issuer Precincts (this is the precinct that issued the ticket)
 a) select violprec, count(violprec) as violprec_frequency from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by violprec order by violprec_frequency desc limit 5;
 b) select issuerprec, count(issuerprec) as issuerprec_frequency from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by issuerprec order by violprec_frequency desc limit 5;


4.) Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes?
>> select violcode, issuerprec, count(sumnum) as no_of_tickets from parking_csv_partion_year where parking_csv_partion_year.issue_year=2017 group by issuerprec, violcode order by no_of_tickets desc limit 3;

5.) Find out the properties of parking violations across different times of the day: The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
>>  udf:
	import sys
	for line in sys.stdin:
	if line[-1] == "A":
    		print(line[:2],":",line[2:4])
	elif line[-1]=="P" and line[:2]=="12":
		print(line[:2],":",line[2:4])
	else:
    		print(str(int(line[:2])+12),":",line[2:4])

>> select transform(violtime) using "python time_udf.py" as (viotime string)from parking_csv_partition_year where parking_csv_partion_year.issue_year=2017

6) Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations
>>  couldnt perform the query because python is not working in my cloudera setup. For the same i hv requested for the doubt clearance session.


