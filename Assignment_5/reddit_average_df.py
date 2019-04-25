#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 5 (Question 1 - reddit_average_df.py)

#  Submission Date: 10th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker



import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
from pyspark.sql.types import StringType
import json
spark = SparkSession.builder.appName('Reddit ETL').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+




def main(inputs, output):
    comments_schema = types.StructType([ # commented-out fields won't be read
    types.StructField('archived', types.BooleanType(), True),
    types.StructField('author', types.StringType(), True),
    types.StructField('author_flair_css_class', types.StringType(), True),
    types.StructField('author_flair_text', types.StringType(), True),
    types.StructField('body', types.StringType(), True),
    types.StructField('controversiality', types.LongType(), True),
    types.StructField('created_utc', types.StringType(), True),
    types.StructField('distinguished', types.StringType(), True),
    types.StructField('downs', types.LongType(), True),
    types.StructField('edited', types.StringType(), True),
    types.StructField('gilded', types.LongType(), True),
    types.StructField('id', types.StringType(), True),
    types.StructField('link_id', types.StringType(), True),
    types.StructField('name', types.StringType(), True),
    types.StructField('parent_id', types.StringType(), True),
    types.StructField('retrieved_on', types.LongType(), True),
    types.StructField('score', types.LongType(), True),
    types.StructField('score_hidden', types.BooleanType(), True),
    types.StructField('subreddit', types.StringType(), True),
    types.StructField('subreddit_id', types.StringType(), True),
    types.StructField('ups', types.LongType(), True),
    #types.StructField('year', types.IntegerType(), False),
    #types.StructField('month', types.IntegerType(), False),
    ])
    comments = spark.read.json(inputs, schema=comments_schema).repartition(8)
    averages = comments.groupby('subreddit').agg(functions.avg(comments['score']))
    averages.explain()
    averages.write.csv(output, mode='overwrite')

if __name__ == '__main__':# make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
