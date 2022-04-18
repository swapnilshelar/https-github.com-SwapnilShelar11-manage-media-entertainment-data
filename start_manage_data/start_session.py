"""
Start spark session
"""
import logging
from pyspark.sql import SparkSession

logging.info("spark session starting...")
spark = SparkSession \
    .builder \
    .config("spark.jars","C:\\Program Files (x86)\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.25.jar")\
    .master("local") \
    .getOrCreate()
