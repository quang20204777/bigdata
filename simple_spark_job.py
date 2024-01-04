from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("SimpleSparkJob").getOrCreate()

# Đường dẫn đến tệp văn bản
input_file_path = "hdfs://namenode:9000/user/root/input/yelp_academic_dataset_review.json"

# Đọc dữ liệu từ tệp văn bản thành DataFrame
data = spark.read.format("json").load(input_file_path)

# Hiển thị 5 dòng đầu tiên
data.show(5, truncate=False)

# Thực hiện một số xử lý đơn giản - ở đây chỉ là đếm số dòng
line_count = data.count()
print(f"Total lines in the file: {line_count}")

# Tắt SparkSession
spark.stop()
