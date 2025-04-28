import pyspark
from pyspark.sql import SparkSession

class Transformation:

    def __init__(self,spark):
        self.spark=spark

    def transform_ETL(self,df):

        print("Transformation in pyspark is in process")

        df1=df.na.drop()
        df1.show()
        return df1.show()
