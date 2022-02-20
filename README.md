# Data Lake with Amazon S3
In this project, we will explain how to build an ETL pipeline to extracts data from S3 buckets, transforms data and load back into a set of dimensional and fact tables in S3 bucket for analytics of Sparkify streaming musics application.

## Data Sources :
The data stored in 2 places that reside in S3 :
- [Song_data](s3://udacity-dend/song_data) : JSON files covered the main activities for the users of the music app
- [Log_data](s3://udacity-dend/log_data) : JSON metadata about users

After ETL processing, the set of tables will be stored in S3 bucket with public access in parquet format.

## Data modeling :
The data modeling are the same that [Cloud_Data_Warehouse](https://github.com/Iaddiop/Cloud_Data_Warehouse/blob/master/README.md). We have data modeling based on star schema :
- **Fact Table**

**- songplays** : song plays (we will filtering data for page == NextSong), have these columns :
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- **Dimension Tables**

**- users** : users table have this columns : user_id, first_name, last_name, gender, level

**- songs** : songs, have these columns : song_id, title, artist_id, year, duration

**- artists** : artists, have these columns : artist_id, name, location, latitude, longitude

**- time** : timestamps of records in songplays broken down into specific units, have these columns : start_time, hour, day, week, month, year, weekday

## Data Lake pipeline processing options:
- **Option 1 : S3 + Spark on local mode** : for testing only not use for production
![image info](./diagram1.png)
- **Option 2 : S3 + Spark in EMR cluster on standalone mode** :
![image info](./diagram.png)

## How to run this project :
To run this project, please following the below steps :

1 - Create EMR cluster

2 - Copy and paste the `etl.py` script to the EMR cluster at home directory

3 - Then lunch the etl script : run `etl.py` by submiting spark job to process data (extract, transform and insert data to the S3 bucket), make sure the S3 target bucket have public permission

## The expected results :

**- songplays** : 333 rows

**- users** : 105 rows

**- songs** : 14896 rows

**- artists** : 10025 rows

**- time** : 333 rows

### References :
- Data types : https://spark.apache.org/docs/latest/sql-ref-datatypes.html
