"""
From here run code
"""
import configparser
import logging

from logic_manage_data.countrywise_data import country_data_manage
from logic_manage_data.data_cleaning import dataframe_cleaning
from logic_manage_data.duration_data import manage_duration_data
from logic_manage_data.rating_data import rating_data_manage
from read import read_data

config = configparser.ConfigParser()
config.read("..\\config.properties")
data_base=config.get('ReadSection','data_base')
table_name=config.get('ReadSection','table_name')
user=config.get('ReadSection','user')
password=config.get('ReadSection','password')
write_path=config.get('WriteSection','writePath')

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler = logging.FileHandler('logs.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

netflix_df=dataframe_cleaning(read_data(data_base,table_name,user,password))
logging.info("Data ready for transformation...")
country_data_manage(netflix_df,write_path)
rating_data_manage(netflix_df,write_path)
manage_duration_data(netflix_df,write_path)
