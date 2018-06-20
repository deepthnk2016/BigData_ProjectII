#!/bin/bash

jobid=`cat /home/maria_dev/project/logs/currentbatchno.txt`

LOGFILE=/home/maria_dev/project/logs/log_batch_$jobid

#--------------------------------------------------
# Create lookup tables in HBase
#--------------------------------------------------
echo "Creating LookUp Tables - Start" >> $LOGFILE

echo "create 'Station_Geo_Map', 'geo'" | hbase shell
echo "create 'Subscribed_Users', 'subscn'" | hbase shell
echo "create 'Song_Artist_Map', 'artist'" | hbase shell

echo "Creating LookUp Tables - End" >> $LOGFILE

#--------------------------------------------------
# Read from files are populate the lookup tables
#--------------------------------------------------

#-------------------------------------
#Populate table Station_Geo_Map
#-------------------------------------
file="/home/maria_dev/project/Lookup/stn-geocd.txt"
while IFS= read -r line
do
 stnid=`echo $line | cut -d',' -f1`
 geocd=`echo $line | cut -d',' -f2`
 echo "put 'Station_Geo_Map', '$stnid', 'geo:geo_cd', '$geocd'" | hbase shell
done <"$file"
#-------------------------------------
#Populate table Song_Artist_Map
#-------------------------------------
file="/home/maria_dev/project/Lookup/song-artist.txt"
while IFS= read -r line
do
 songid=`echo $line | cut -d',' -f1`
 artistid=`echo $line | cut -d',' -f2`
 echo "put 'Song_Artist_Map', '$songid', 'artist:artistid', '$artistid'" | hbase shell
done <"$file"
#---------------------------------
#Populate table Subscribe_Users
#---------------------------------
file="/home/maria_dev/project/Lookup/user-subscn.txt"
while IFS= read -r line
do
 userid=`echo $line | cut -d',' -f1`
 startdt=`echo $line | cut -d',' -f2`
 enddt=`echo $line | cut -d',' -f3`
 echo "put 'Subscribed_Users', '$userid', 'subscn:startdt', '$startdt'" | hbase shell
 echo "put 'Subscribed_Users', '$userid', 'subscn:enddt', '$enddt'" | hbase shell
done <"$file"

#Call hive script to populate User_Artist_Map table
hive -f /home/maria_dev/project/scripts/user-artist.hql
