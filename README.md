  # CSV to Parquet Data Pipeline with PySpark and PostgreSQL
     This project presents a robust data pipeline leveraging PySpark and PostgreSQL to process CSV data efficiently,
     store it in a PostgreSQL database, conduct SQL-based filtering operations, transform it into Parquet format, 
     and finally store the processed data back to an S3 bucket.

# Prerequisites
 - **Before you begin, ensure you have the following installed on your machine:**
    - [Python-3.7.0](https://www.python.org/downloads/release/python-370/)
    - [AWS CLI-2.15.18](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-version.html)
    - [Spark-3.5.0](https://spark.apache.org/downloads.html)
    - [Hadoop-3.2.1](https://hadoop.apache.org/release/3.2.1.html_)
    - [PostgreSQL-16.1](https://www.postgresql.org/download/windows/)

# Overview
  The data pipeline consists of the following main steps:

  **Data Ingestion**: The pipeline begins with ingesting CSV files from a specified source. These CSV files are uploaded to an S3 bucket.
  
  **Data Processing with PySpark**: PySpark reads the CSV files directly from the S3 bucket. The data is then processed using 
    PySpark DataFrame APIs for transformations and data cleaning.
  
  **Data Loading into PostgreSQL**: Once the data is processed, it is stored in a PostgreSQL database. This step involves 
    creating a table in the PostgreSQL database and inserting the processed data into this table using PySpark.
  
  **Data Retrieval and Filtering**: After the data is stored in the PostgreSQL database, it can be queried using SQL queries.
    This enables data filtering and selection based on specific criteria.
  
  **Parquet Conversion**: The filtered data is then converted into the Parquet file format, which offers advantages such as
    efficient storage, compression, and columnar storage.
  
  **Storage in S3**: Finally, the Parquet files are stored back in the S3 bucket. Each Parquet file and its corresponding
    S3 folder are named based on the current timestamp to ensure data versioning and traceability.

# Suggestions/Contributions

  - Suggestions to this POC are welcome! If you have any ideas for improvements, new features, feel free to open an issue or submit a pull request.
  
