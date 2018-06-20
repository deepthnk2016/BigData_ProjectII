#!/bin/bash

batchid=`cat /home/maria_dev/project/logs/currentbatchno.txt`
LOGFILE=/home/maria_dev/project/logs/log_batch_$batchid


echo "Running hive script for data enrichment and filtering..." >> $LOGFILE

hive -hiveconf batchid=$batchid -f /home/maria_dev/project/scripts/data_enrichment.hql


