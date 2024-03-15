from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit
import yaml
import boto3
import datetime
import pandas as pd



#This function reads data from Postgres, converts it into parquet format, and dumps it on an s3 bucket 
def PG_to_S3_from_yml(yml_file_path):
    try:
        with open(yml_file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        spark = SparkSession.builder.appName("PG-to-S3").getOrCreate()

        postgres_url = "jdbc:postgresql://localhost:5432/postgres"
        properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
        }
        query = f"(SELECT * FROM {config['table_name']}) as subquery"
        df = spark.read.jdbc(url=postgres_url, table=query, properties=properties)

        df.printSchema()
        today_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = f"s3a://{config['target_bucket']}/shared/parquet_{today_date}"
        df.coalesce(2).write.mode("overwrite").parquet(file_path)
    except:
        print("this function is not able to read data from postgres and dump into S3")
    #upload_parquet_files_to_s3(config)
    # df_part2.repartition(2).write.mode("overwrite").parquet(file2_path)
    # df_part1.write.format("parquet").option("compression","none").save(file1_path)
    # df_part1.write.csv(file1_path, mode='overwrite')
    # df_part1.coalesce(1).write.mode('overwrite').format("csv").option("header","true")\
    # .option("delimiter",delimiter).save(file1_path)

    # df_part1.to_parquet(file1_path, index =False)
    # df_part2.to_parquet(file2_path, index =False)

    # s3_client = boto3.client(
    #     's3',
    #     aws_access_key_id=config['aws_access_key_id'],
    #     aws_secret_access_key=config['aws_secret_access_key']
    # )

    # s3_client.upload_file(config['parquet_file'], config['target_bucket'], file_path)

    




# yaml_file_path = 'C:/Users/manissharma/Downloads/Pyspark-TRP/configs/PG_to_S3.yml'
# PG_to_S3_from_yml(yaml_file_path)
