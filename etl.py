import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
import os
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

'''
Config parser section to get login detail to AWS
'''
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creating spark session for ETL pipeline
    :return: spark object to connect to Spark clusters
    '''
    print('create_spark_session')
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


spark = create_spark_session()


def process_song_data(spark, input_data, output_data):
    '''
    Processing song data from client S3.
    :param spark: used as connection endpoint
    :param input_data: client S3 coordinates
    :param output_data: target S3 to house parquete output
    :return: Correctly formatted and built tables stored as parquet files on target S3
    '''

    song_data = input_data + 'song_data/A/A/*/*.json'
    df = spark.read.json(song_data)
    df.drop_duplicates()

    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration')
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').\
        parquet(output_data + '/songs_table_dump.parquete')

    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude')
    artists_table.write.mode('overwrite').parquet(output_data + '/artist_table_dump.parquete')


def process_log_data(spark, input_data, output_data):
    '''
    Processing log data from client S3.
    :param spark: used as connection endpoint
    :param input_data: client S3 coordinates
    :param output_data: target S3 to house parquete output
    :return: Correctly formatted and built tables stored as parquet files on target S3
    '''

    log_data = input_data + 'log_data/2018/11/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong').drop_duplicates()

    users_table = df.select('userId',
                            'firstName',
                            'lastName',
                            'gender',
                            'level')
    users_table.write.mode('overwrite').parquet(output_data + '/users_table_dump.parquete')

    df = df.withColumn('start_time', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'y-M-d kk:mm:ss'))
    df = df.withColumn('hour', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'H'))
    df = df.withColumn('day', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'd'))
    df = df.withColumn('week', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'w'))
    df = df.withColumn('month', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'M'))
    df = df.withColumn('year', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'y'))
    df = df.withColumn('weekday', F.date_format((df.ts / 10000).cast(dataType=T.TimestampType()), 'E'))

    time_table = df.select('start_time',
                           'hour',
                           'day',
                           'week',
                           'month',
                           'year',
                           'weekday')
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + '/time_table_dump.parquete')

    #re-reading song data for songplays table join
    song_data = input_data + 'song_data/A/A/*/*.json'
    song_df = spark.read.json(song_data)
    song_df.drop_duplicates()

    conditions = [df.song == song_df.title, df.artist == song_df.artist_name, df.length == song_df.duration,\
                  df.page == 'NextSong']
    df = df.join(song_df, conditions, how = 'left_outer')

    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    songplays_table = df.select('songplay_id',
                                'start_time',
                                'userId',
                                'level',
                                'song_id',
                                'artist_id',
                                'sessionId',
                                'location',
                                'userAgent')
    songplays_table.withColumn('year', year('start_time')).withColumn('month', month('start_time'))\
        .write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + '/songplays_table_dump.parquete')

def main():
    '''
    Main function to run whole etl pipeline for Data Lake
    :return: none
    '''

    print('main')
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = datetime.today().strftime('%Y-%m-%d') #prefix to this function should be target S3 storage coordinates

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
