import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    initiate sparkSession (configure and create or get application)
    To deal with datasets and dataframe
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description : this function will be use to process data from song log files

    Arguments :
        - spark : spark session function
        - input_data : url to access to the data from S3 bucket for song log files
        - output_data : url to acces to the public S3 bucket for output parquet files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), \
                          col('artist_name').alias('name'), \
                          col('artist_location').alias('location'), \
                          col('artist_latitude').alias('latitude'), \
                          col('artist_longitude').alias('longitude')).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    Description : this function will be use to process data from log users files

    Arguments :
        - spark : spark session function
        - input_data : url to access to the data from S3 bucket for log users files
        - output_data : url to acces to the public S3 bucket for output parquet files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'
    #log_data = input_data + 'log_data/2018/11/2018-11-01-events.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').cast('int').alias('user_id'), \
                        col('firstName').alias('first_name'),\
                        col('lastName').alias('last_name'), \
                        col('gender'), \
                        col('level')).dropDuplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('ts', (F.round(col('ts')/1000)).cast("timestamp"))

    # extract columns to create time table
    time_table = df.selectExpr('ts AS start_time').dropDuplicates().orderBy('start_time', ascending=False) \
                .withColumn('hour', F.hour('start_time')) \
                .withColumn('day', F.dayofmonth('start_time'))\
                .withColumn('week', F.weekofyear('start_time')) \
                .withColumn('month', F.month('start_time')) \
                .withColumn('year', F.year('start_time')) \
                .withColumn('weekday', F.dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.withColumn('songplay_id', F.monotonically_increasing_id()).join(song_df, (song_df.title == df.song))\
                            .select('songplay_id',\
                           col('ts').alias('start_time'),\
                           col('userId').alias('user_id'),\
                           'level',\
                           'song_id',\
                           'artist_id',\
                           col('sessionId').alias('session_id'),\
                           'location',\
                           col('userAgent').alias('user_agent'))
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table \
                .withColumn("year", F.year(col("start_time"))) \
                .withColumn("month", F.month(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-simple-storage/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
