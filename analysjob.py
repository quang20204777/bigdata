import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from pyspark.sql.types import DateType

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Indexing to Elasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12-7.12.0,commons-httpclient:commons-httpclient:3.1") \
    .config("spark.jars", "./elasticsearch-spark-30_2.12-7.12.0.jar,./commons-httpclient-3.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

# Đường dẫn đến file CSV
file_path = "hdfs://namenode:9000/universal_top_spotify_songs.csv"

df = spark.read.option("header", "true").csv(file_path)
df.printSchema()

columns_to_drop = ["danceability", "key", "loudness", "mode", "speechiness",
                    "acousticness", "instrumentalness", "liveness", "tempo", "time_signature"]
df = df.drop(*columns_to_drop)

# 2. Thêm cột năm trích xuất từ snapshot_date
df = df.withColumn("snapshot_date", col("snapshot_date").cast(DateType()))  # Chuyển đổi snapshot_date thành kiểu dữ liệu ngày tháng
df = df.withColumn("extracted_year", year("snapshot_date"))

# 3. Lọc ra những bản ghi có country khác rỗng
df = df.filter(col("country").isNotNull())

# Hiển thị kết quả
df.show(10, truncate=False)

df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.resource", "spotify/_doc") \
    .option("es.mapping.id", "spotify_id") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save("spotify/_doc")

# Stop the Spark session
spark.stop()
