import json
import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, ArrayType

class StreamProcessing:

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
        self.__db_host = 'ec2-18-232-24-132.compute-1.amazonaws.com'
        self.__db_port = 5432
        self.__db_name = "earthquake"
        self.__db_user = "postgres"
        self.__db_pass = "nidheesh"
        self.__db_url = "jdbc:postgresql://" + self.__db_host + \
                        ':' + str(self.__db_port) + '/' +self.__db_name
        self.__table_name = "Ereadings"
        self.__properties = {
            "driver": "org.postgresql.Driver",
            "user": self.__db_user,
            "password": self.__db_pass
        }
        self.__write_mode = "append"

    def initialize_stream(self):

        self.kafkastream = KafkaUtils.createDirectStream(self.sparkstream,\
                           [self.kafka_topic], {"bootstrap.servers": self.kafka_brokers})
        self.lines = self.kafkastream.map(lambda x: json.loads(x[1]))
        self.lines.foreachRDD(lambda lines: self.process_stream(lines))

    def process_stream(self, rdd):

        if rdd.isEmpty():
            print("no incoming data")

        else:

            data_frame = rdd.toDF().cache()
            array_mean = udf(lambda x: float(np.mean(x)), FloatType())
            def square_list(array_list):
                return [float(val)**2 for val in array_list]
            square_list_udf = udf(lambda y: square_list(y), ArrayType(FloatType()))
            df_square = data_frame.select('*', square_list_udf('x').alias("sq_x"), \
                   square_list_udf('y').alias("sq_y"), square_list_udf('z').alias("sq_z"))
            df_average = df_square.select("*", array_mean("sq_x").alias("avg_x"), \
                  array_mean("sq_y").alias("avg_y"), array_mean("sq_z").alias("avg_z"))
            final_df = df_average.select("*", pow(col("avg_x")+ col("avg_y")+ \
                      col("avg_z"), 0.5).alias("gal"))
            final_df.write.jdbc(url=self.__db_url, table=self.__table_name, \
            mode=self.__write_mode, properties=self.__properties)


    def start_process(self):

        self.initialize_stream()
        self.sparkstream.start()
        self.sparkstream.awaitTermination()


if __name__ == "__main__":

    PROCESSING = StreamProcessing()
    PROCESSING.start_process()
