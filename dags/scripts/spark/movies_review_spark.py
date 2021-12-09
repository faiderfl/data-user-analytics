from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, expr, when

#Classification Movie Review Logic


sc = SparkSession.builder.appName("Read_movies_review").config("spark.hadoop.fs.s3a.access.key", "AKIAWS5YIEDA2PHYS3RO").config("spark.hadoop.fs.s3a.secret.key", "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").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate() 

movies_reviews= sc.read.option("inferSchema", "false").options(header='true',sep=",").csv("s3a://raw-data-bootcamp/movie_review.csv")
movies_reviews.show(5)

# Tokenize text
tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
df_tokens = tokenizer.transform(movies_reviews).select("cid", "review_token")

# Remove stop words
remover = StopWordsRemover(inputCol="review_token", outputCol="review_clean")
df_clean = remover.transform(df_tokens).select("cid", "review_clean")

df_clean = df_clean.withColumn("positive_review", when(expr("array_contains(review_clean, 'good')"), 1).otherwise(0))

# function to check presence of good
df_out = df_clean.select("cid","positive_review")
#array_contains(df_clean.review_clean, "good").alias("positive_review")
df_out.write.csv(path='s3a://staging-data-bootcamp/reviews.csv',mode="overwrite",header = 'true')