# Data Lake project

## Background
A music streaming startup is planning to move from data warehouse into proper Data Lake. Files that are stored on AWS S3 storage are in .json format. Those files consist of songs data and log data. Startup planing to get more efficient way of analyzing this information.

## Scope
Scope for this project os to build ETL pipeline that:
1. Fetch files from S3 repos
2. Build appropriate table schemas
3. Perform initial data wrangling where appropriate
4. Store target tables on designated S3 storage in parquet format

## Infrastructure flow
 
|startup S3|  -> |ETL pipe (python over EMR cluster)| -> |target S3| 

## Files
1. README.md - description of project
2. etl.py - ETL pipeline written in python/pyspark
3. dl.cfg - credentials to AWS platform (empty for public)

## Usage
This project use sample S3 buckets that are public.
etl.py file should be run from EMR cluster with Spark installed.
dl.cfg is credential file and should be kept secret.

After setting up AWS S3 target bucket and EMR cluster etl.py should be run on this cluster either via console or local machine (then dl.cfg need to be correctly filled up). Script will fetch .json files for songs data and log data (may take  while!) then create target tables and store them as parquete files in target S3. S3 coordinates are setup in etl.py file. </br>
WARNING! For project purpose whole infrastructure should be setup in us-west-2 region (Oregon) otherwise fetching data from initial S3 may not be possible or efficient enough!