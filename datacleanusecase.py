from pyspark.sql import *
from pyspark.sql.functions import *
8
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

#data loading
data="C:\\Users\\admin\\Desktop\\data.csv"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
#df.printSchema()
#yyyy-MM-dd  format only
#data cleaning
#clean header and make the data stuctural.
#data cleansing steps
def dynamic_date(col,frmts=("yyyy-MM-dd","dd-MMM-yy","dd-MM-yyyy","dd-MMM-yy","MM-dd-yyyy","MMM/yyyy/dd")):
    return coalesce(*[to_date(col,i)for i in frmts])

import re
cols=[re.sub('[^a-zA-Z0-9]','',c) for c in df.columns]
ndf=df.toDF(*cols)
res=ndf.withColumn("dobbirthdate",dynamic_date(col("dobbirthdate")))
res.printSchema()
res.show()

#data processing
res.createOrReplaceTempView("tab")
result=spark.sql("select * from tab where dobbirthdate>' 2020-03-22'")
result.show()





