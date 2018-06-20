/***********************************************/
/* Author: Deepak Ray                         */
/* Date: 18/06/2018                           */
/* Project: Acadgild Music Data Analysis      */
/**********************************************/
/* Data Analysis                              */
/**********************************************/

//import the required packages
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.hadoop.hbase.util.Bytes


//Create HiveContext
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

import hiveContext.implicits._

//Read the passed batchid as parameter
val args=sc.getConf.get("spark.driver.args").split("\\s+")
val batchid=args(0)


//Set the output path where all output files will be generated
val outputdirpath="/maria_dev/project/batch"+batchid

//Set the database to be used in hive
hiveContext.sql("use project")

/**************************** Problem 1 - Start ***********************/
//Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

val df1=hiveContext.sql("select station_id,count(distinct song_id),count(distinct user_id) from enriched_data where status='pass' and batchid='"+batchid+"'group by station_id")
df1.repartition(1).write.option("header","true").csv(outputdirpath+"/top10stations")

/**************************** Problem 1 - End ***********************/

/**************************** Problem 2 - Start ***********************/
//Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'

val df2=hiveContext.sql("SELECT CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type,SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data ed LEFT OUTER JOIN subscribed_users su ON ed.user_id=su.user_id WHERE ed.status='pass' AND ed.batchid='"+batchid+"' GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END")

//Write output of the above query to csv file
df2.repartition(1).write.option("header","true").csv(outputdirpath+"/total_songs_played_byeach_usertype")

/**************************** Problem 2 - End ***********************/

/**************************** Problem 3 - Start ***********************/
//Determine top 10 connected artists. Connected artists are those whose songs are most listened by the unique users who follow them.

val df3=hiveContext.sql("SELECT ua.artist_id,COUNT(DISTINCT ua.user_id) AS user_count FROM(SELECT user_id, artist_id FROM user_artist_map LATERAL VIEW explode(artists_array) artists AS artist_id ) ua INNER JOIN(SELECT artist_id, song_id, user_id FROM enriched_data WHERE status='pass' AND batchid='"+batchid+"') ed ON ua.artist_id=ed.artist_id AND ua.user_id=ed.user_id GROUP BY ua.artist_id ORDER BY user_count DESC LIMIT 10")

//Write output of the above query to csv file
df3.repartition(1).write.option("header","true").csv(outputdirpath+"/top_10_connected_artists")

/**************************** Problem 3 - End ***********************/

/**************************** Problem 4 - Start ***********************/
//Determine top 10 songs who have generated the maximum revenue

val df4 = hiveContext.sql("SELECT song_id,SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data WHERE status='pass' AND batchid='"+batchid+"' AND (like=1 OR song_end_type=0)GROUP BY song_id ORDER BY duration DESC LIMIT 10")

//Write output of the above query to csv file
df4.repartition(1).write.option("header","true").csv(outputdirpath+"/top_10_songs")

/**************************** Problem 4 - End ***********************/

/**************************** Problem 5 - Start ***********************/
//Determine top 10 unsubscribed users who listened to the songs for the longest duration


val df5=hiveContext.sql("SELECT ed.user_id,SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data ed LEFT OUTER JOIN subscribed_users su ON ed.user_id=su.user_id WHERE ed.status='pass' AND ed.batchid='"+batchid+"' AND (su.user_id IS NULL OR (CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))))GROUP BY ed.user_id ORDER BY duration DESC LIMIT 10")

//Write output of the above query to csv file
df5.repartition(1).write.option("header","true").csv(outputdirpath+"/top_10_unsubscribed_songs")

/**************************** Problem 5 - End ***********************/

System.exit(0)
