"""
Read data from respective path
"""
import logging

from start_session import spark

def read_data(data_base, table_name,user,password):
    """
    Connect and Read data from MySQL database
    :param data_base:
    :param table_name:
    :param user:
    :param password:
    :return: read data from MySQL database - table
    """

    logging.info("File reading...")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    return spark.read \
         .format("jdbc") \
         .option("url", f"jdbc:mysql://localhost:3306/{data_base}") \
         .option("driver", "com.mysql.jdbc.Driver") \
         .option("dbtable", table_name) \
         .option("user", user) \
         .option("password", password) \
         .load()
