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

#importing all tables
qry="(select table_name from information_schema.tables where TABLE_SCHEMA='mysql1db') t "

df1=spark.read.format("jdbc").option("url",host)\
    .option("dbtable",qry).option("user",user)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .load()
#to convert these tables in list
tabs=[x[0] for x in df1.collect()]
df1.show()

for i in tabs:
    df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable",i).option("user",user)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .load()
    df.show()









'''
#reading tables from a list
tabs=['csvclean1','csvclean2','emp','empclean']

for i in tabs:
    df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable",i).option("user",user)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .load()
    df.show()

========================================
qry="(select * from emp where sal>2000) t"
df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable",qry).option("user",user)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .load()
df.show()
======================================================

'''






