from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
import configparser
from configparser import ConfigParser
conf=ConfigParser()

conf.read(r"C:\\Users\\admin\\Desktop\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")

#data="C:\\Users\\admin\\Downloads\\10000Records.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").load(data)

import re
cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)

ndf.show(5)





ndf.write.format("jdbc").option("url",host)\
    .option("dbtable","csvclean2").option("user",user)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .save()

