from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, expr, when, sum,count, current_timestamp, round



sc = SparkSession.builder.appName("User behavior metrics").getOrCreate()

reviews= sc.read.option("inferSchema", "false").options(header='true',sep=",").csv("s3a://staging-data-bootcamp/reviews.csv/")
#reviews.show(5)


user_purchase= sc.read.option("inferSchema", "false").options(header='true',sep=",").csv("s3a://staging-data-bootcamp/user_purchase.csv/")
#user_purchase.show(5)


user_behavior = user_purchase.groupBy(user_purchase.customer_id)\
                                    .agg(sum(user_purchase.quantity * user_purchase.unit_price))\
                                    .withColumnRenamed('sum((quantity * unit_price))','amount_spent')\
                                    .withColumn('amount_spent',round('amount_spent',6))

reviews_behavior = reviews.groupBy(reviews.cid)\
                                    .agg(sum(reviews.positive_review),count(reviews.cid))\
                                    .withColumnRenamed('sum(positive_review)','review_score')\
                                    .withColumnRenamed('count(cid)','review_count')

user_behavior_metrics =user_behavior.join(reviews_behavior, on=user_behavior.customer_id==reviews_behavior.cid).withColumn('insert_date',current_timestamp())

user_behavior_metrics.select('customer_id','amount_spent','review_score','review_count','insert_date').write.csv(path='s3a://production-data-bootcamp/user_behavior_metrics.csv',mode="overwrite",header = 'true')