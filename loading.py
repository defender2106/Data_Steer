import  pyspark
from pyspark.sql import SparkSession

class Loading:
    def __init__(self,spark):
        self.spark=spark

    def save_data(self,df):
        print("load the data")
        df.write.option("header","true").csv("file:////C:/Bigdata_Mithun/Pyspark/Documents/DATASETS/usdata2.csv")


