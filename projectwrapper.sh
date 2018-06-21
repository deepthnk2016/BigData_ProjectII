#!/bin/bash


scriptpath=/home/maria_dev/project/scripts

#Generate web input files
python $scriptpath/generate_web_data.py

#Generate mobile input files
python $scriptpath/generate_mob_data.py

#Call script to format the data
sh $scriptpath/formatdata.sh

#Load formatted data to hive tables
sh $scriptpath/loadformatteddata.sh

#Load HBase tables to Hive Tables
sh $scriptpath/data_enrichment_lookup.sh

#Apply Validation and Data Enrichment Rules
sh $scriptpath/apply_data_enrichment.sh

#Perform Data Analysis
sh $scriptpath/data_analysis.sh
