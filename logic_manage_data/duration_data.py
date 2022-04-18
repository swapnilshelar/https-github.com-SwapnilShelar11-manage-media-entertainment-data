"""
Usecase 1:
   Get average Movie duration per country
   Data - Netflix data
   Data should be sorted in descending order by average duration
   output should contain country name and average duration per country

Usecase 2:
   Get Total count of TV Shows for each season duration
   Data - Netflix data
   Data should be sorted in descending order by count of TV Shows
   output should contain season duration and count of TV Shows
"""
import logging
from pyspark.sql.functions import col, avg, split, round


def manage_duration_data(netflix_df, write_path):
    """
    Duration data transformation
    :param netflix_df:
    :param write_path:
    :return: Written output in respective csv file
    """
    duration_movie_data = netflix_df.filter(col("type") == "Movie") \
        .withColumn("dura", split(col("duration"), " ")[0].cast("int")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name")) \
        .agg(round(avg(col("dura")), 2).alias("avg_duration")) \
        .orderBy(col("avg_duration").desc())

    duration_tv_df = netflix_df.filter(col("type") == "TV Show").groupBy(col("duration")).count() \
        .orderBy(col("count").desc())

    logging.info("Duration data transformation: ")
    logging.info("1. Get average Movie duration per country: ")
    logging.info("2. Get Total count of TV Shows for each season duration: ")
    duration_movie_data.show()
    duration_tv_df.show()

    duration_movie_data.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}duration_data//duration_movie_df")
    duration_tv_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}duration_data//duration_tv_df")
