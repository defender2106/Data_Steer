from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("first").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("Error")

data = [(1,"chinna",2000),(2,"djdjjd",3000),(3,"djdjjd",4000),(2,"djdjjd",10000)]
mdata=["id","name","salary"]
df=spark.createDataFrame(data=data,schema=mdata)
df.show()

second_salary = df.select("salary").distinct().orderBy("salary",ascending=False).limit(2).collect()[1][0]
print(f"second salary:{second_salary}")
