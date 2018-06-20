-------------------------------------------------------------
-- Created By: Deepak Ray
-- Date: 18/06/2018
-- Project: Acadgild Music Data Analysis
-------------------------------------------------------------
-- Load formatted data from Mod and Web directories  to hive.
-------------------------------------------------------------
USE project;

--Use this statement to avoid reserve word exception
SET hive.support.sql11.reserved.keywords=false;

--Create the table
CREATE TABLE IF NOT EXISTS formatted_data1
(
User_id STRING,
Song_id STRING,
Artist_id STRING,
Timestamp STRING,
Start_ts STRING,
End_ts STRING,
Geo_cd STRING,
Station_id STRING,
Song_end_type INT,
Like INT,
Dislike INT
)
PARTITIONED BY
(batchid INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

--Load web data from formattedweb folder
LOAD DATA INPATH '/maria_dev/project/batch${hiveconf:batchid}/formattedweb/'
INTO TABLE formatted_data1 PARTITION (batchid=${hiveconf:batchid});

--Load mobile data from Mob folder
LOAD DATA INPATH '/maria_dev/project/batch${hiveconf:batchid}/Mob/'
INTO TABLE formatted_data1 PARTITION (batchid=${hiveconf:batchid});

