# Import the necessary modules
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

packages = ["org.elasticsearch:elasticsearch-spark-30_2.12-7.12.0", 
            "commons-httpclient:commons-httpclient:3.1", 
           ]

jars_path = ["./elasticsearch-spark-30_2.12-7.12.0.jar", 
             "./commons-httpclient-3.1.jar", 
            ]
# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ElasticsearchIndexCreation") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.jars", ",".join(jars_path)) \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "test/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

ES_NODES = "elasticsearch"
ES_RESOURCE = "test/_doc"

# Create a DataFrame from JSON data with explicit schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), False),
    StructField("City", StringType(), True)
])

json_data = [
    {"Name": "Minh Đăng", "Age": 22, "City": "HCM"},
    {"Name": "Văn Thành", "Age": 22, "City": "HCM"},
    {"Name": "Việt Anh", "Age": 22, "City": "HCM"},
    {"Name": "Văn Hiếu", "Age": 22, "City": "HCM"},
    {"Name": "Trung Kiên", "Age": 22, "City": "HCM"}
]

json_df = spark.read.json(spark.sparkContext.parallelize(json_data), schema=schema)

# Write the DataFrame to Elasticsearch, triggering index creation
json_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", ES_NODES) \
    .option("es.resource", ES_RESOURCE) \
    .option("es.mapping.id", "Name") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("append") \
    .save(ES_RESOURCE)

# Stop the Spark session
spark.stop()
