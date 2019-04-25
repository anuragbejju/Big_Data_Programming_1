#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 7 (Question 2 - load_logs_spark.py)

#  Submission Date: 26th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SQLContext
from pyspark import SparkContext, SparkConf
import sys,os
import datetime
import re, string
import uuid

conf = SparkConf().setAppName('pyspark')
sc = SparkContext(conf=conf)
sqlc=SQLContext(sc)
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def log_split(line):

    #split log and pass values
    updated_line = line_re.split(line)
    if len(updated_line) >= 4:
        if ((len(updated_line[1]) > 0) and (len(updated_line[2]) > 0) and (len(updated_line[3]) > 0) and (len(updated_line[3]) > 0)):
            #Formatting date
            date_time_value = datetime.datetime.strptime(updated_line[2], "%d/%b/%Y:%H:%M:%S")
            return Row(str(uuid.uuid1()), updated_line[1],date_time_value,updated_line[3],int(updated_line[4]))

def main(input_directory,keyspace,table_name):

    # connecting to keyspace
    cluster_seeds = ['199.60.17.188', '199.60.17.216']

    # Creating spark session
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    # Initializing Schema for the inputed table
    data_frame_schema = types.StructType([
    types.StructField('id', types.StringType(), False),
    types.StructField('host', types.StringType(), False),
    types.StructField('date_time', types.DateType(), False),
    types.StructField('path_value', types.StringType(), False),
    types.StructField('bytes', types.IntegerType(), False),])

    # Reading file and repartitioning it
    text = sc.textFile(input_directory).repartition(8)

    # Getting the input values
    input_values = text.map(log_split).filter(lambda x: x is not None)

    # Creating DataFrame and passing values
    df=sqlc.createDataFrame(input_values, data_frame_schema)

    #Writing dataframe to cassandra table
    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).save()


if __name__ == '__main__':
    input_directory = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_directory,keyspace,table_name)
