from pyspark.sql import SparkSession
import boto3
import yaml

def local_to_S3_from_yaml(yaml_file_path):
    try:
        with open(yaml_file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        aws_access_key_id = config['aws_access_key_id']
        aws_secret_access_key = config['aws_secret_access_key']
        s3_bucket_name = config['source_bucket']
        s3_key = config['s3_key']
        local_csv_path = config['local_csv_path']

        spark = SparkSession.builder.appName("local-to-S3").getOrCreate()

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        s3_client.upload_file(local_csv_path, s3_bucket_name, s3_key)
    except:
        print("there is error in your solution")



    # yaml_file_path = 'C:/Users/manissharma/Downloads/Pyspark-TRP/configs/credentails.yml'
    # local_to_S3_from_yaml(yaml_file_path)



