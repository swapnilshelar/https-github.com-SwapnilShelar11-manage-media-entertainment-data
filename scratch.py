from pyspark.sql import SparkSession
import configparser
import os
from pyspark.sql.functions import col

spark=SparkSession.builder.master("local").getOrCreate()

config = configparser.ConfigParser()
config.read(os.environ['CONFIGFILE'])
readPath=config.get('ReadSection','readPath')
fileFormat=config.get('ReadSection','fileFormat')
df=spark.read\
    .option("header",True)\
    .format(fileFormat) \
    .option("inferSchema",True)\
    .load("C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\Hello-word\\netflix_titles.csv")

cdf=df.filter(~col("rating").isin("November 1, 2020","2019","2017","Adriane Lenox",
                             "Richard Pepple%","Shavidee Trotter","Jowharah Jones%","2006","84 min","Itziar Aizpuru","Heather McDonald",
                             "Benn Northover","Jide Kosoko","74 min","Maury Chaykin","2021","Rachel Dratch","66 min","Kristen Schaal%"))
cdf1=cdf.filter(~col("rating").like("Richard%"))
cdf2=cdf1.filter(~col("rating").like("Jowharah%"))
cdf3=cdf2.filter(~col("rating").like("Keppy%"))
cdf4=cdf3.filter(~col("rating").like("Kristen%"))
cdf4.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv("C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\Hello-word\\write")
