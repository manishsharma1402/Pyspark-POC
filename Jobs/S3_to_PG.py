from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import boto3
import yaml

def read_yml_file(file_path):
    with open(file_path, 'r') as file:
        cred = yaml.safe_load(file)
        return cred

file_path = 'C:/Users/manissharma/Downloads/Pyspark-TRP/configs/S3_to_PG.yaml'
yml_data = read_yml_file(file_path)

def S3_to_PG_from_yml(yml_file_path):
    with open(yml_file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)

    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']
    s3_bucket_name = config['source_bucket']
    s3_key = config['s3_key']
    local_csv_path = config['local_csv_path']

    spark = SparkSession.builder.appName("S3-to-PG").getOrCreate()
    sc = SQLContext(spark)

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)

    path = f's3a://{s3_bucket_name}/{s3_key}'
    df = spark.read.option("header","true").csv(path)

    df.show()
    sc.registerDataFrameAsTable(df, yml_data['df_table'])
    df_table=sc.sql(yml_data['source_sql'])
    df_table.show()

    postgres_url = "jdbc:postgresql://localhost:5432/postgres"
    properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
    }
    df_table.write.jdbc(url=postgres_url, table=yml_data['table'], mode="overwrite", properties=properties)



# yaml_file_path = 'C:/Users/manissharma/Downloads/Pyspark-TRP/configs/credentails.yml'
# S3_to_PG_from_yml(yaml_file_path)

