from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lower, regexp_replace, to_json, struct
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import StringType
import nltk
from nltk.stem import SnowballStemmer

# Initialize NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

# UDF for stemming
def stem_text(text):
    stemmer = SnowballStemmer('english')
    return ' '.join([stemmer.stem(word) for word in text.split()])

stem_udf = udf(stem_text, StringType())


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataPreprocessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Kafka Configuration for Reading
kafka_topic_read = "reddit_topic_gaza"
kafka_bootstrap_servers = "localhost:9092" # or your Kafka server

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_read) \
    .load()

# Assuming the value in Kafka topic is of String type and contains the text data
df_string = df.selectExpr("CAST(value AS STRING)")

# NLP transformations
# 1. Lowercase and remove special characters
df_cleaned = df_string.withColumn("text", lower(regexp_replace(col("value"), "[^a-zA-Z\\s]", "")))

# 2. Tokenization
tokenizer = Tokenizer(inputCol="text", outputCol="words")
df_words = tokenizer.transform(df_cleaned)

# 3. Stopwords Removal
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_words)

# 4. Stemming
df_stemmed = df_filtered.withColumn("stemmed_text", stem_udf(col("filtered_words")))

# Kafka Configuration for Writing
kafka_topic_write = "data_preprocessed"

# Convert DataFrame to JSON string
df_json = df_stemmed.select(to_json(struct("*")).alias("value"))

# Output to Kafka
query = df_json.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic_write) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()
