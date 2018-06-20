------------------------------------------------------------------------------
-- Created By: Deepak Ray
-- Date: 18/06/2018
-- Project: Acadgild Music Data Analysis
-------------------------------------------------------------------------------
-- Perform data enrichment on formatted_data based on the following conditions:
-- 1. If like is null put 0
-- 2. If dislike is 0 put 0
-- 3. If artist_id is null fetch it from song_artist_map table based on song_id
-- 4. If geo_cd is null fetch it from station_geo_map table based on station_id
-------------------------------------------------------------------------------

SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.sql11.reserved.keywords=false;

USE project;
------------------------------------------------------------
-- Create a temporary table to apply the enrichment rules
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enriched_data_temp
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
STORED AS ORC;

INSERT OVERWRITE TABLE enriched_data_temp
PARTITION (batchid)
SELECT
i.user_id,
i.song_id,
sa.artist_id,
i.timestamp,
i.start_ts,
i.end_ts,
sg.geo_cd,
i.station_id,
I.song_end_type,
(CASE WHEN i.like IS NULL THEN 0 ELSE i.like END) AS like,
(CASE WHEN i.dislike IS NULL THEN 0 ELSE i.dislike END) AS dislike,
i.batchid AS status
FROM formatted_data i
LEFT OUTER JOIN station_geo_map sg ON i.station_id = sg.station_id
LEFT OUTER JOIN song_artist_map sa ON i.song_id = sa.song_id
WHERE i.batchid=${hiveconf:batchid};

-----------------------------------------------------------------
-- Create the final enrichment table which will fetch data from
-- the above table and will have a column to mark if a row of
-- data is passed or failed.
-----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enriched_data
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
(batchid INT,
status STRING)
STORED AS ORC;


INSERT OVERWRITE TABLE enriched_data
PARTITION (batchid, status)
SELECT 
i.user_id,
i.song_id,
i.artist_id,
i.timestamp,
i.start_ts,
i.end_ts,
i.geo_cd,
i.station_id,
i.song_end_type,
i.like,
i.dislike,
i.batchid,
(CASE WHEN(i.like=1 AND i.dislike=1) 
OR i.user_id IS NULL 
OR i.song_id IS NULL
OR i.timestamp IS NULL
OR i.start_ts IS NULL
OR i.end_ts IS NULL
OR i.geo_cd IS NULL
OR i.user_id='' 
OR i.song_id='' 
OR i.timestamp='' 
OR i.start_ts='' 
OR i.end_ts='' 
OR i.geo_cd=''
OR i.artist_id IS NULL
OR i.artist_id='' THEN 'fail' ELSE 'pass' END) AS status
FROM enriched_data_temp i 
WHERE i.batchid=${hiveconf:batchid};
