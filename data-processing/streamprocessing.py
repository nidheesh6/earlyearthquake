"""processing the streams from kafka"""
import json
import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, ArrayType
import postgres
sys.path.append("./database-scripts/")

class StreamProcessing:

   """initializing the spark""" 
    def __init__(self):
        self.spark_config = SparkConf()
        self.sparkcontext = SparkContext(appName="PythonSparkStreamingKafka", \
                            conf=self.spark_config)
        self.sparkcontext.setLogLevel("WARN")
        self.sparkstream = StreamingContext(self.sparkcontext, 1)
        self.spark = SparkSession(self.sparkcontext)
        self.kafka_topic = "pressure_sensor"
        self.kafka_brokers = "3.210.67.129:9092"
        SparkSession.builder.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar')
        
   """initializing the streams to read from kafka"""
    def initialize_stream(self):

        self.kafkastream = KafkaUtils.createDirectStream(self.sparkstream,\
                           [self.kafka_topic], {"bootstrap.servers": self.kafka_brokers})
        self.lines = self.kafkastream.map(lambda x: json.loads(x[1]))
        self.lines.foreachRDD(lambda lines: self.process_stream(lines))
    """processing the data received from kafka"""
    def process_stream(self, rdd):

        if rdd.isEmpty():
            print("no incoming data")

        else:
	    """convert the data from rdd to dataframes"""
            data_frame = rdd.toDF().cache()
	    """calculating the mean of an array""""
            array_mean = udf(lambda x: float(np.mean(x)), FloatType())
	    """calculating the square of each element in an array"""
            def square_list(array_list):
                return [float(val)**2 for val in array_list]
            square_list_udf = udf(lambda y: square_list(y), ArrayType(FloatType()))

	    """adding new columns to the dataframe"""
            df_square = data_frame.select('*', square_list_udf('x').alias("sq_x"), \
                   square_list_udf('y').alias("sq_y"), square_list_udf('z').alias("sq_z"))

            df_average = df_square.select("*", array_mean("sq_x").alias("avg_x"), \
                  array_mean("sq_y").alias("avg_y"), array_mean("sq_z").alias("avg_z"))

            """calculating the gal value for predicting earthquake and wrting to the data frame"""

            final_df = df_average.select("*", pow(col("avg_x")+ col("avg_y")+ \
                      col("avg_z"), 0.5).alias("gal"))

	    """writing the data to the postgres"""
            try:
                connector = postgres.PostgresConnector(
                    "ec2-18-232-24-132.compute-1.amazonaws.com", "earthquake", "postgres", "nidheesh")
                connector.write(final_df, "Ereadings", "append")

            except Exception as error:
                print(error)
                pass

    def start_process(self):
	"""starting the spark process to read data from kafka brokers"""
        self.initialize_stream()
        self.sparkstream.start()
        self.sparkstream.awaitTermination()


if __name__ == "__main__":

    PROCESSING = StreamProcessing()
    PROCESSING.start_process()
