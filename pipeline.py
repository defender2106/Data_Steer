import pyspark
import logging
from pyspark.sql import SparkSession

import ingestion
import transformation
import loading


class Pipeline:
    def run_etl_pipeline(self):
        logging.info("Staring the pipeline")


        ingest_process =ingestion.Ingest(self.spark)
        df=ingest_process.ingest_data()

        transform_phase = transformation.Transformation(self.spark)
        logging.info("etl_transformation")
        transform_df=transform_phase.transform_ETL(df)
        logging.info("etl_transformation2")

        persist_process =loading.Loading(self.spark)
        persist_process.save_data(transform_df)

    def create_spark_session(self):
        self.spark=SparkSession.builder.appName("project").getOrCreate()

if __name__=="__main__" :

    logging.basicConfig(level=logging.INFO)
    logging.info("Application Started")

    pipeline=Pipeline()
    pipeline.create_spark_session()
    pipeline.run_etl_pipeline()