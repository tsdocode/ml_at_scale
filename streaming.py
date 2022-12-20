import os
os.environ['PYSPARK_SUBMIT_ARGS'] = \
  '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql.functions import *
from pyspark.sql.types import *
from firebase import RealtimeDB


db = RealtimeDB()

spark = SparkSession \
    .builder \
    .appName("streaming") \
    .getOrCreate()

from pyspark.ml import PipelineModel
trained_model = PipelineModel.load("pipeline")


userid = "ixlwq92s"
password = "pn7Vq0psXudW1-Rr3JsAwMQMbgAWi0oO"
host = "dory-01.srvs.cloudkafka.com:9094,dory-02.srvs.cloudkafka.com:9094,dory-03.srvs.cloudkafka.com:9094"
topic = "ixlwq92s-network_instrusion3"
sasl_mech = "SCRAM-SHA-256"

streamingInputDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host)  \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(userid, password)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", sasl_mech) \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .option("subscribe", topic) \
  .load()


schema = StructType([ 
        StructField('duration', FloatType(), True),
        StructField('protocol_type', StringType(), True),
        StructField('service', StringType(), True),
        StructField('flag', StringType(), True),
        StructField('src_bytes', FloatType(), True),
        StructField('dst_bytes', FloatType(), True),
        StructField('land', FloatType(), True),
        StructField('wrong_fragment', FloatType(), True),
        StructField('urgent', FloatType(), True),
        StructField('hot', FloatType(), True),
        StructField('num_failed_logins', FloatType(), True),
        StructField('logged_in', FloatType(), True),
        StructField('num_compromised', FloatType(), True),
        StructField('root_shell', FloatType(), True),
        StructField('su_attempted', FloatType(), True),
        StructField('num_root', FloatType(), True),
        StructField('num_file_creations', FloatType(), True),
        StructField('num_shells', FloatType(), True),
        StructField('num_access_files', FloatType(), True),
        StructField('num_outbound_cmds', FloatType(), True),
        StructField('is_host_login', FloatType(), True),
        StructField('is_guest_login', FloatType(), True),
        StructField('count', FloatType(), True),
        StructField('srv_count', FloatType(), True),
        StructField('serror_rate', FloatType(), True),
        StructField('srv_serror_rate', FloatType(), True),
        StructField('rerror_rate', FloatType(), True),
        StructField('srv_rerror_rate', FloatType(), True),
        StructField('same_srv_rate', FloatType(), True),
        StructField('diff_srv_rate', FloatType(), True),
        StructField('srv_diff_host_rate', FloatType(), True),
        StructField('dst_host_count', FloatType(), True),
        StructField('dst_host_srv_count', FloatType(), True),
        StructField('dst_host_same_srv_rate', FloatType(), True),
        StructField('dst_host_diff_srv_rate', FloatType(), True),
        StructField('dst_host_same_src_port_rate', FloatType(), True),
        StructField('dst_host_srv_diff_host_rate', FloatType(), True),
        StructField('dst_host_serror_rate', FloatType(), True),
        StructField('dst_host_srv_serror_rate', FloatType(), True),
        StructField('dst_host_rerror_rate', FloatType(), True),
        StructField('dst_host_srv_rerror_rate', FloatType(), True)
    ])

aggregation = streamingInputDF.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
df = aggregation.select(col("parsed_value.*"))

df = df.withColumn('class', lit('normal').cast(StringType()))
df = df.na.fill(value=0)
df = df.na.drop()


aggregation = streamingInputDF.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
df = aggregation.select(col("parsed_value.*"))
df = trained_model.transform(df)
df = df.withColumn('key', lit('data').cast(StringType()))
df = df.select(col("key"),to_json(struct("*"))).toDF("key", "value")

# pd_df = df.toPandas()

# data_list = pd_df.to_dict(orient='record')

# print(data_list)


# query = df\
# .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()


query = df \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host)  \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(userid, password)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", sasl_mech) \
  .option("topic", "ixlwq92s-network_analysis") \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .start() \
  .awaitTermination()


