#!/bin/bash

#Get the batch id for logging purpose
batchid=`cat /home/maria_dev/project/logs/currentbatchno.txt`
LOGFILE=/home/maria_dev/project/logs/log_batch_$batchid

echo "Running spark script for data analysis..." >> $LOGFILE

#Add Hbase Classpath
hbase_path=`hbase classpath`

#Remove, if any directory already exists for output
hadoop fs -rm -r /maria_dev/project/batch$batchid

#invoke the spark shell script
spark-shell -i /home/maria_dev/project/scripts/data_analysis.scala --conf spark.driver.args=$batchid --jars /usr/hdp/2.6.4.0-91/hbase/lib/hive-hbase-handler-1.2.1000.2.6.4.0-91.jar,$hbase_path

echo "Incrementing batchid for the next run..." >> $LOGFILE

#Finally, increment the batch id, once process is over
batchid=`expr $batchid + 1`
echo -n $batchid > /home/maria_dev/project/logs/currentbatchno.txt
