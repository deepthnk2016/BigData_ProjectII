#!/bin/bash

#-------------------------------------------
#Fetch the batch id
#-------------------------------------------
batchid=`cat /home/maria_dev/project/logs/currentbatchno.txt`
LOGFILE=/home/maria_dev/project/logs/log_batch_$batchid


echo "Placing data files from local to HDFS..." >> $LOGFILE
#------------------------------------------
#Delete the existing folders if exists
#------------------------------------------
hadoop fs -rm -r /maria_dev/project/batch${batchid}/Web/
hadoop fs -rm -r /maria_dev/project/batch${batchid}/formattedweb/
hadoop fs -rm -r /maria_dev/project/batch${batchid}/Mob/
#------------------------------------------
#Again Create the folder structures
#------------------------------------------
hadoop fs -mkdir -p /maria_dev/project/batch${batchid}/Web/
hadoop fs -mkdir -p /maria_dev/project/batch${batchid}/Mob/

#-----------------------------------------------------------------------
#Put the xml file and the txt file from Web and Mobile directory to HDFS 
#-----------------------------------------------------------------------
hadoop fs -put /home/maria_dev/project/Web/* /maria_dev/project/batch${batchid}/Web/
hadoop fs -put /home/maria_dev/project/Mob/* /maria_dev/project/batch${batchid}/Mob/

echo "Running pig script for data formatting..." >> $LOGFILE

#------------------------------------------
#Invoke the pig script to format xml to txt
#------------------------------------------
pig -param batchid=$batchid /home/maria_dev/project/scripts/formatdata.pig

echo "Running hive script for formatted data load..." >> $LOGFILE

