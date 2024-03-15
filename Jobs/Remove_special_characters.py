from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName("remove-special-characters").getOrCreate()
sc = SQLContext(spark)
schema = StructType([
    StructField("Col1", StringType(), True),
    StructField("Col2", StringType(), True),
    StructField("Col3", StringType(), True),
    StructField("Col4", StringType(), True),
    StructField("Col5", StringType(), True)
])


data = [["AB%", "BC#", "CD@", "DE%", "EF$"],
        ["AB-", "BC!", "CD*", "DE%", "EF#"],
        ["AB&", "BC^", "CD$", "DE#", "EFG)"],
        ["GH(", "IJ&$%", "KL#@", "MN*", "OP$"]
        ]

df = spark.createDataFrame(data,schema)
# df.show()
sc.registerDataFrameAsTable(df, "peoplesoft")
df_table=sc.sql("select * from peoplesoft")
# df_table.show()


query = """
    SELECT
        TRIM(BOTH '-%$@#!^&*()[]{}<>"'' ' FROM Col1) AS Col1,
        TRIM(BOTH '-%$@#!^&*()[]{}<>"'' ' FROM Col2) AS Col2,
        TRIM(BOTH '-%$@#!^&*()[]{}<>"'' ' FROM Col3) AS Col3,
        TRIM(BOTH '-%$@#!^&*()[]{}<>"'' ' FROM Col4) AS Col4,
        TRIM(BOTH '-%$@#!^&*()[]{}<>"'' ' FROM Col5) AS Col5
    FROM peoplesoft
"""

df_table=sc.sql(query)
df_table.show()
