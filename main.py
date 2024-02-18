from Jobs.local_to_S3 import local_to_S3_from_yaml
from Jobs.S3_to_PG import S3_to_PG_from_yml
from Jobs.PG_to_S3 import PG_to_S3_from_yml


local_to_S3_from_yaml('C:/Users/manissharma/Downloads/Pyspark-TRP/configs/credentails.yml')
S3_to_PG_from_yml('C:/Users/manissharma/Downloads/Pyspark-TRP/configs/credentails.yml')
PG_to_S3_from_yml('C:/Users/manissharma/Downloads/Pyspark-TRP/configs/PG_to_S3.yml')
