from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, expr, when


sc = SparkSession.builder.appName("Read user purchase").getOrCreate()

df = sc.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://database-bootcamp.cp3hnitxa4mu.us-east-1.rds.amazonaws.com:5432/bootcamp") \
    .option("dbtable", "bootcampdb.user_purchase") \
    .option("user", "dbuser") \
    .option("password", "dbpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show(5)
df_out= df.limit(10)
df.write.csv(path='s3a://staging-data-bootcamp/user_purchase.csv',mode="overwrite",header = 'true')