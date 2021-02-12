import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Creates a Spark session.
            Parameters:
                    None.
            Returns:
                    spark: Spark Session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Reads JSON files, extracts the songs and artists tables, and writes to parquet files.
            Parameters:
                    spark: Spark Session
                    input_data: S3 location of song_data
                    output_data: S3 location where results are stored
            Returns:
                    None.
    '''
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    print("Read Song Data.")

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration'].drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_location = output_data + "/songs/songs.parquet"
    songs_table.write.partitionBy('year','artist_id').parquet(songs_output_location).mode("overwrite")
    
    print("Wrote songs table.")

    # extract columns to create artists table
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'].drop_duplicates()
    
    # write artists table to parquet files
    artists_output_location = output_data + "/artists/artists.parquet"
    artists_table.write.parquet(artists_output_location).mode("overwrite")
      
    print("Wrote artists table.")


def process_log_data(spark, input_data, output_data):
    '''
    Reads JSON files, extracts the songplays, users, and time tables, and writes to parquet files.
            Parameters:
                    spark: Spark Session
                    input_data: S3 location of song_data
                    output_data: S3 location where results are stored
            Returns:
                    None.
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    print("Read log data.")

    # filter by actions for song plays
    df = df.filter(df['page'] == "NextSong")

    # extract columns for users table    
    users_table = df['userId','firstName','lastName','gender','level'].drop_duplicates()
    
    # write users table to parquet files
    users_output_location = output_data + "/users.parquet"
    users_table.write.parquet(users_output_location).mode("overwrite")
    
    print("Wrote users table.")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000),dataType=TimestampType())
    df = df.withColumn("start_time",get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
            .withColumn("day",day("start_time"))\
            .withColumn("week",week("start_time"))\
            .withColumn("month",month("start_time"))\
            .withColumn("year",year("start_time"))\
            .withColumn("weekday",dayofweek("start_time"))\
            .select("start_time","hour","day","week","month","year","weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_output_location = output_data + "/time.parquet"
    time_table.write.partitionBy('year','month').parquet(time_output_location).mode("overwrite")
    
    print("Wrote time table.")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data,"/songs/songs.parquet")
    
    print("Read songs.parquet.")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,df("artist") == song_df("artist_id"),"inner")\
                    .withColumn("songplay_id",monotonically_increasing_id())\
                    .select("songplay_id","start_time","userId","level","song_id","artist_id","sessionId","location","userAgent").drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_output_location = output_data + "/songplays.parquet"
    songplays_table.write.partitionBy('year','month').parquet(songplays_output_location).mode("overwrite")
    
    print("Wrote songplays table.")


def main():
    '''
    Main function of this script. Loads data from S3, processes the tables with Spark, and writes the tables.
            Parameters:
                    None.
            Returns:
                    None.
    '''
    spark = create_spark_session()
    
    print("Created Spark Session.")
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-project4-neagan"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
