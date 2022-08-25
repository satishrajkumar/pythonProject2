from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:mysql://mysql1.czctf7x2imqz.ap-south-1.rds.amazonaws.com:3306/mysql1db"
uname="myuser"
pwd="mypassword"

df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","emp").option("user",uname)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .load()

#process data
res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))

res.write.format("jdbc").option("url",host)\
    .option("dbtable","empclean").option("user",uname)\
    .option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .save()





res.show()









'''
#error= java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
#db?useSSL=false

#.withColumn("hiredate",to_date(col("hiredate"),"yyyy-MM-dd"))

#process data
res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))
'''