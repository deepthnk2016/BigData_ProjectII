#!/bin/bash

batchid=`cat /home/maria_dev/project/logs/currentbatchno.txt`
LOGFILE=/home/maria_dev/project/logs/log_batch_$batchid

echo "Creating hive tables on top of hbase tables for data enrichment and filtering..." >> $LOGFILE

hive -f /home/maria_dev/project/scripts/load_lookupdata_hive.hql
