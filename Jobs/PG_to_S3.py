from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit
import yaml
import boto3
import datetime
import pandas as pd


def PG_to_S3_from_yml(yml_file_path):
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

    condition_column = config['condition_column']
    condition_value = config['condition_value']

    df_with_part_number = df.withColumn(
    "part_number",
    coalesce(lit(1), lit(2)).alias("part_number")
    )

    df_part1 = df_with_part_number.filter(f"{condition_column} < {condition_value}")
    df_part2 = df_with_part_number.filter(f"{condition_column} >= {condition_value}")

    df_part1.show()
    df_part2.show()

    # df_part1 = df_part1.toPandas()
    # df_part2 = df_part2.toPandas()

    today_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    config['parquet_file1'] = config['parquet_file1'].replace('{{DATE}}', today_date)
    config['parquet_file2'] = config['parquet_file2'].replace('{{DATE}}', today_date)

    file1_path = f'shared/parquet_part_1_{today_date}'
    file2_path = f'shared/parquet_part_2_{today_date}'
    # df_part1.write.parquet(file1_path)
    df_part1.repartition(2).write.mode("overwrite").parquet(file1_path)
    df_part2.repartition(2).write.mode("overwrite").parquet(file2_path)
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

    # s3_client.upload_file(config['parquet_file1'], config['target_bucket'], file1_path)
    # s3_client.upload_file(config['parquet_file2'], config['target_bucket'], file2_path)



yaml_file_path = 'C:/Users/manissharma/Downloads/Pyspark-TRP/configs/PG_to_S3.yml'
PG_to_S3_from_yml(yaml_file_path)