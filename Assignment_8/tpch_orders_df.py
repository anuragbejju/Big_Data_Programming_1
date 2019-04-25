#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 8 (Question 2 - tpch_orders_df.py)

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
spark = SparkSession.builder.appName('TPCH Order DF').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(x):
	orderkey, price, names = x
	namestr = ', '.join(sorted(list(names)))
	return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def load_table(key_space,table):
	loaded_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
	return loaded_df

def main(keyspace,out_dir,orderkeys):

    keys = str(tuple(orderkeys))

    # loading orders table where the order key is in the keys list inputted by the user
    orders = load_table(keyspace,'orders').where('orderkey in' + keys)
    orders.registerTempTable('orders')

    # loading lineitem table
    line_item = load_table(keyspace,'lineitem')
    line_item.registerTempTable('lineitem')

    # loading part table
    part = load_table(keyspace,'part')
    part.registerTempTable('part')

    # join all the 3 tables and select out orderkey, totalprice and part name
    result = spark.sql("SELECT o.orderkey,o.totalprice,p.name FROM orders o JOIN lineitem l ON (o.orderkey = l.orderkey)  JOIN part p ON (p.partkey = l.partkey)")

    # group by the order key and total price and use collect_set to get the part names associated with the order key as a set
    result = result.groupBy(result['o.orderkey'],result['o.totalprice']).agg(functions.collect_set(result['p.name'])).orderBy(result['o.orderkey'], ascending=True)

    #convert to RDD
    output = result.rdd.map(tuple)

    #Get the values in the required format
    output = output.map(output_line)

    #Save as file
    output.saveAsTextFile(out_dir)
    result.explain()

#orderkey, price, names
if __name__ == "__main__":
	keyspace = sys.argv[1]
	out_dir = sys.argv[2]
	orderkeys = sys.argv[3:]
	main(keyspace,out_dir,orderkeys)
