# Data Lake Project
My solution for the data lake  AWS project for the Data Engineering Nanodegree on Udacity

## Quick Start
Edit dl.cfg: Fill in AWS acces key (KEY) and secret (SECRET).

Copy log_data and song_data folders to your own S3 bucket.

Create output_data folder in your S3 bucket.

run etl.py

NOTE: It is recommended to run script in the AWS cluster and use S3 input files. The regions of S3 and cluster should be the same (save communication time).

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Task is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

 Test database and ETL pipeline by running queries given by the analytics team from Sparkify and compare the results with their expected results.

### Datasets
uploaded in AWS S3

Song data: s3://udacity-dend/song_data

Log data: s3://udacity-dend/log_data

### Schema for Song Play Analysis

Using the song and log datasets, we create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table - songplays
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables - users, songs, artists, time
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

