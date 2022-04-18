"""
Usecase:
   Get count for each rating worldwide, United states, India
   Data - Netflix data
   Data should be sorted in descending order by count
   Output should be contains rating and count
   1.Get count for each rating worldwide
   2.Get count for each rating United States
   3.Get count for each rating India
"""
import logging
from pyspark.sql.functions import col


def rating_data_manage(netflix_df, write_path):
    """
    Rating Data transformations
    :param netflix_df:
    :param write_path:
    :return: Written output in respective csv file
    """
    rating_df = netflix_df.groupBy(col("rating")).count() \
        .orderBy(col("count").desc())
    rating_us_df = netflix_df.filter(col("country").like("United States")) \
        .groupBy(col("rating")).count() \
        .orderBy(col("count").desc())
    rating_india_df = netflix_df.filter(col("country").like("India")) \
        .groupBy(col("rating")).count() \
        .orderBy(col("count").desc())

    logging.info("Rating data transformation: ")
    logging.info("1. Get count for each rating worldwide: ")
    logging.info("2. Get count for each rating United States: ")
    logging.info("3. Get count for each rating India: ")
    rating_df.show()
    rating_us_df.show()
    rating_india_df.show()

    rating_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}rating_data//rating_df")
    rating_us_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}rating_data//rating_us_df")
    rating_india_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}rating_data//rating_india_df")
