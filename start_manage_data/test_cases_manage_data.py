"""
Unit testing in python using pytest
"""
from start_session import spark

WRITE_PATH = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\pyspark_practice\\" \
             "managed-media-entertainment-data\\netflix_data\\write\\"


def test_counterywise_data():
    """
    Unit test case for countrywise data
    :return: Result pass or fail
    """
    result_countrywise_data = spark.read.option("header", True) \
        .format("csv") \
        .option("inferSchema", True) \
        .load(f"{WRITE_PATH}countrywise_data//result_country_df") \
        .collect()[0][1]
    assert result_countrywise_data == 3194


def test_duration_data():
    """
    Unit test case for duration data
    :return: Result pass or fail
    """
    result_duration_data = spark.read.option("header", True) \
        .format("csv") \
        .option("inferSchema", True) \
        .load(f"{WRITE_PATH}duration_data//duration_tv_df") \
        .collect()[0][1]
    assert result_duration_data == 1789


def test_rating_data():
    """
    Unit test case for duration data
    :return: Result pass or fail
    """
    result_rating_data = spark.read.option("header", True) \
        .format("csv") \
        .option("inferSchema", True) \
        .load(f"{WRITE_PATH}rating_data//rating_df") \
        .collect()[0][1]
    assert result_rating_data == 3195
