#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 5 (Question 2 - wikipedia_popular_df.py)

#  Submission Date: 10th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName('Reddit ETL').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


def path_to_hour(path):
  t = (path.split('/'))[-1] #split and get last element
  return t[11:22] #get string based on the index


def main(inputs, output):
    input_data_schema = types.StructType([
    types.StructField('language', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('count', types.LongType(), True),
    types.StructField('bytes', types.LongType(), True)
    ])
    path_udf = udf(path_to_hour, StringType())
    input_data = spark.read.csv(inputs, schema=input_data_schema,sep=' ').withColumn('filename', path_udf(functions.input_file_name())) # read space-delimited files with a sep argument
    filtered_input_data = input_data.filter((input_data.language=='en')&(input_data.title!='Main_Page')&(~input_data.title.startswith('Special:'))).cache() # filter to get only English Wikipedia pages,  Not the page titled 'Main_Page' and  Not the titles starting with 'Special:'.
    selected_input_data = filtered_input_data.select('filename', 'count') #Filter to get only filename and count
    grouped_input_data = selected_input_data.groupby('filename').agg(functions.max(input_data['count'])).withColumnRenamed("MAX(count)", "max_count") #get largest number of page views in each hour.
    joined_input_data = filtered_input_data.join(broadcast(grouped_input_data), (grouped_input_data['filename'] == filtered_input_data['filename'])).drop(filtered_input_data.filename) # broadcast join two tbles
    filtered_joined_input_data = joined_input_data.filter(joined_input_data['count'] == joined_input_data['max_count']).sort('filename','title', ascending=True) #count == max(count) for that hour.
    selected_joined_input_data = filtered_joined_input_data.select('filename','title','count') #Filter to get filename, title and count
    selected_joined_input_data.show(40, False)
    selected_joined_input_data.write.json(output, mode='overwrite') #Write out as json

if __name__ == '__main__':# make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
