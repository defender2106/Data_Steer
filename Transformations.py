from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("first").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("Error")

data = [(1,"chinna"),(2,"djdjjd")]
mdata=["id","name"]
df=spark.createDataFrame(data=data,schema=mdata)
df.show()
