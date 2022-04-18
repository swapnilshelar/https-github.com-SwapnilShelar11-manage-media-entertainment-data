"""
Usecase:
   Get number of Movies and TV Shows for each country
   Data - Netflix Data
   Data should be sorted in descending order by count
   Output should contain country name, count of Movies and TV Shows, percentages in each country  
   1.Get total count and percentages for each country
   2.Get total count and percentage of Movies for each country 
   3.Get total count and percentage of TV Shows for each country 
"""
import logging
from pyspark.sql.functions import col, split, round


def country_data_manage(netflix_df, write_path):
    """
    :param netflix_df:
    :param write_path:
    :return: Written output in respective csv file
    """
    result_country_df = netflix_df.filter((col("country") != "No Data")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    result_country_movie_df = netflix_df.filter((col("type").like("Movie"))) \
        .groupBy(split(col("country"), ",")[0].alias("country_name"), col("type")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    result_country_tvshow_df = netflix_df.filter(col("type").like("TV Show")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name"), col("type")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    logging.info("Country data transformation: ")
    logging.info("1. Get total count and percentages for each country: ")
    logging.info("2. Get total count and percentage of Movies for each country: ")
    logging.info("3. Get total count and percentage of TV Shows for each country: ")

    result_country_df.show()
    result_country_movie_df.show()
    result_country_tvshow_df.show()

    result_country_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_df")
    result_country_movie_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_movie_df")
    result_country_tvshow_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_tvshow_df")
