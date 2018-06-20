#!/bin/bash

#-------------------------------------------
#Fetch the batch id
#-------------------------------------------
batchid=`cat /home/maria_dev/project/logs/currentbatchno.txt`
LOGFILE=/home/maria_dev/project/logs/log_batch_$batchid

#Invoke the hive script
hive -hiveconf batchid=$batchid -f /home/maria_dev/project/scripts/load_formatted_data.hql
