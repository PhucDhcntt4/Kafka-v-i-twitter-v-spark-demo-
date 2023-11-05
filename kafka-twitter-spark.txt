Trên máy chủ Kafka, tạo một topic có tên là tweets.

kafka-topics --create --zookeeper localhost:2181 --topic tweets
Bước 2: Cài đặt Spark

Trên máy chủ Spark, cài đặt Spark và các thư viện cần thiết.

pip install pyspark
pip install twitter
Bước 3: Viết chương trình Spark

Chương trình Spark sau sẽ phân tích dữ liệu tweet và xác định cảm xúc của chúng.

Python
import pyspark
from pyspark.sql import SparkSession
from twitter import TwitterAPI

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("Twitter Sentiment Analysis").getOrCreate()

# Tạo một consumer Kafka
consumer = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "tweets").load()

# Lấy dữ liệu tweet từ Kafka
tweets = consumer.select("value")

# Phân tích cảm xúc của tweet
def get_sentiment(tweet):
    # Xóa các ký tự đặc biệt
    tweet = tweet.lower().replace("@", "").replace("#", "")

    # Xác định cảm xúc của tweet
    if any(word in tweet for word in ["vui", "hạnh phúc", "tuyệt vời", "thích", "tốt", "tuyệt", "tuyệt vời", "yêu thích", "khá", "tốt"]):
        return "positive"
    elif any(word in tweet for word in ["buồn", "thất vọng", "tồi tệ", "ghét", "tệ", "tồi tệ", "tồi tệ", "tồi tệ", "không tốt"]):
        return "negative"
    else:
        return "neutral"

# Áp dụng hàm get_sentiment() cho dữ liệu tweet
sentiments = tweets.rdd.map(get_sentiment)

# Xuất dữ liệu sentiment
sentiments.saveAsTextStream("output")

# Chạy chương trình
spark.start()
Vui lòng thận trọng khi sử dụng mã. Tìm hiểu thêm
Bước 4: Chạy chương trình

Chạy chương trình Spark bằng lệnh sau:

spark-submit --master local[2] --deploy-mode client --py-files tình cảm_analysis.py tình cảm_analysis.py

Chương trình sẽ bắt đầu đọc dữ liệu tweet từ Kafka và phân tích cảm xúc của chúng. Dữ liệu sentiment sẽ được xuất ra một file văn bản có tên là output.

Kết quả

File văn bản output sẽ chứa các dòng sau:

positive
neutral
negative
Dòng đầu tiên cho biết tweet đầu tiên có cảm xúc tích cực, dòng thứ hai cho biết tweet thứ hai có cảm xúc trung lập, và dòng thứ ba cho biết tweet thứ ba có cảm xúc tiêu cực.

Tùy chỉnh chương trình

Chương trình trên có thể được tùy chỉnh để phù hợp với các ứng dụng cụ thể. Ví dụ, bạn có thể thay đổi hàm get_sentiment() để sử dụng các từ khóa khác để xác định cảm xúc của tweet. Bạn cũng có thể lưu trữ dữ liệu sentiment vào một cơ sở dữ liệu hoặc kho lưu trữ để phân tích thêm.