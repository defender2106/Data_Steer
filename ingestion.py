import pyspark
from pyspark.sql import SparkSession

class Ingest:
    def __init__(self,spark):
        self.spark=spark


    def ingest_data(self):
        print("Ingetion data from local source")
        customerdf=self.spark.read.csv("file:////C:/Bigdata_Mithun/Pyspark/Documents/DATASETS/customer_records.csv",header= True)
        customerdf.show()
        return customerdf






