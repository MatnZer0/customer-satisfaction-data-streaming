import time
import os
from kafka import KafkaConsumer
from hcl2 import loads

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import from_json, col

GOOGLE_APPLICATION_CREDENTIALS = "/credentials/google_credentials_project.json"
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
TOPIC_NAME = "messages"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS

def read_terraform_variables(file_path):
    with open(file_path, 'r') as f:
        variables_content = f.read()
    parsed_variables = loads(variables_content)
    terraform_variables = parsed_variables['variable']
    
    return terraform_variables

variables_file_path = '/terraform/variables.tf'
terraform_variables = read_terraform_variables(variables_file_path)

project_id = terraform_variables[1]['project']['default']
bucket_name = terraform_variables[5]['gcs_bucket_name']['default']
data_set_name = terraform_variables[4]['bq_dataset_name']['default']

schema = StructType([
    StructField("unique_id", StringType()),
    StructField("channel_name", StringType()),
    StructField("category", StringType()),
    StructField("sub_category", StringType()),
    StructField("order_id", StringType()),
    StructField("issue_reported_at", StringType()),
    StructField("issue_responded", StringType()),
    StructField("agent_name", StringType()),
    StructField("csat_score", StringType())
])

def start_consumer(bootsstrapServer, maximumRetries=6):
    print("Trying to start consumer!")
    tries = 0
    connectionFound = False
    while tries < maximumRetries and not connectionFound:
        try:
            consumerConnection = KafkaConsumer(
                'messages',
                bootstrap_servers=[bootsstrapServer],
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                group_id= '1'
            )
            connectionFound = True
        except:
            print("Kafka broker not found! Waiting before trying again...")
            time.sleep(5)
            tries += 1
    if tries == maximumRetries:
        print("Something is wrong.")
        raise Exception

    return consumerConnection

def start_spark():
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0',
        'org.apache.kafka:kafka-clients:3.2.0',
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0",
    ]

    spark = SparkSession.builder \
        .appName("spark") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.project.id", project_id) \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", True) \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars", "gcs-connector-hadoop3-2.2.10-shaded.jar") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    return spark

if __name__ == '__main__':
    consumer = start_consumer(KAFKA_BOOTSTRAP_SERVER)
    sparkSession = start_spark()
    for message in consumer:
        json_string = message.value.decode('utf-8')
        df = sparkSession.createDataFrame([(json_string,)], ["value"])

        df = df.select(from_json(col("value"), schema).alias("data")) \
            .select(
                "data.unique_id",
                "data.channel_name",
                "data.category",
                "data.sub_category",
                "data.order_id",
                "data.issue_reported_at",
                "data.issue_responded",
                "data.agent_name",
                "data.csat_score"
            )
        df.show()

        df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_name}.survey-stream") \
        .option("checkpointLocation", f"gs://{bucket_name}/checkpoints/") \
        .option("temporaryGcsBucket", f"{bucket_name}/tmp") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .mode("append") \
        .save()