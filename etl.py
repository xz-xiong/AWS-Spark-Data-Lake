import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create a spark session
    
    Keyword arguments:
    
    Output:
    
        spark -- an spark session
        
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load JSON input data (song_data) from input_data path,
    process the data to extract song_table and artists_table, and
    store the queried data to parquet files.
    
    Keyword arguments:
        spark         -- ref to spark session.
        input_data    -- path to input_data 
        output_data   -- path to store the output 

    Output:
        songs_table   -- directory with parquet files
                       stored in output_data path.
        artists_table -- directory with parquet files
                       stored in output_data path.
                       
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs_table_DF")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_table_DF
        ORDER BY song_id
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    df.createOrReplaceTempView("artists_table_DF")
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM artists_table_DF
        ORDER BY artist_id desc
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')
    return songs_table, artists_table

def process_log_data(spark, input_data, output_data):
    """
    Load JSON input data (log_data) from input_data path,
    process the data to extract users_table, time_table,
    songplays_table, and store the queried data to parquet files.

    Keyword arguments:
        spark         -- ref to spark session.
        input_data    -- path to input_data 
        output_data   -- path to store the output
        
    Output:
        users_table    -- directory with users_table parquet files
                          stored in output_data path.
        time_table     -- directory with time_table parquet files
                          stored in output_data path.
        songplayes_table -- directory with songplays_table parquet files
                            stored in output_data path.
                            
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    df.createOrReplaceTempView("users_table_DF")
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender,
                         level
        FROM users_table_DF
        ORDER BY last_name
    """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts)) 
    
    # extract columns to create time table
    df.createOrReplaceTempView("time_table_DF")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table_DF
        ORDER BY start_time
    """) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data, \
                    'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    # join log_data and song_data DFs
    
    df_ld_sd_joined = df.join(song_df, (df.artist == song_df.artist_name) & \
                    (df.song == song_df.title))
    
    

    # extract columns from joined song and log datasets
    df_ld_sd_joined = df_ld_sd_joined.withColumn("songplay_id", \
                        monotonically_increasing_id())
    df_ld_sd_joined.createOrReplaceTempView("songplays_table_DF")
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table_DF
        ORDER BY (user_id, session_id)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        'year', 'month').parquet(os.path.join(output_data,
                                 'songplays/songplays.parquet'),
                                 'overwrite')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
