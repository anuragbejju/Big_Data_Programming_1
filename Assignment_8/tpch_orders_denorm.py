#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 8 (Question 4 - tpch_orders_denorm.py)

#  Submission Date: 2nd November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SQLContext, Row, SparkSession, functions, types

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('TPCH Order Denorm').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(x):
	orderkey, price, names = x
	namestr = ', '.join(sorted(list(names)))
	return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def load_table(key_space,table):
	loaded_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
	return loaded_df

def main(keyspace,outdir,orderkeys):

    keys = str(tuple(orderkeys))

    # Creating DataFrame and selecting required values
    orders_parts = load_table(keyspace,'orders_parts').where('orderkey in' + keys).select('orderkey','totalprice','part_names').orderBy('orderkey', ascending=True)
    orders_parts.registerTempTable('orders_parts')

    #convert to RDD
    output = orders_parts.rdd.map(tuple)

    #Get the values in the required format
    output = output.map(output_line)

    #Save as file
    output.saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
