#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 8 (Question 3 - tpch_denormalize.py)

#  Submission Date: 2nd November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from cassandra.cluster import Cluster

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('tpch_denormalize').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def load_table(key_space,table):
	loaded_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
	return loaded_df

def main(keyspace, output_keyspace):

    # connecting to keyspace
    session = Cluster(cluster_seeds).connect(output_keyspace)

    # loading orders table
    orders = load_table(keyspace,'orders').cache()
    orders = orders.registerTempTable('orders')

    # loading lineitem table
    line_item = load_table(keyspace,'lineitem')
    line_item.registerTempTable('lineitem')

    # loading part table
    part = load_table(keyspace,'part')
    part.registerTempTable('part')

    # Creating orders_parts table if it doesn't exist and truncating all rows befor loading
    create_statement = 'CREATE TABLE IF NOT EXISTS orders_parts (orderkey int,custkey int,orderstatus text,totalprice decimal,orderdate date,order_priority text,clerk text,ship_priority int,comment text,part_names set<text>, PRIMARY KEY (orderkey));'
    session.execute(create_statement);
    truncate_statemet = 'TRUNCATE orders_parts;'
    session.execute(truncate_statemet)

    # Joining all three tables to get desired output
    join_statement = 'SELECT o.orderkey, p.name FROM orders o JOIN lineitem l ON o.orderkey==l.orderkey JOIN part p ON p.partkey == l.partkey'
    join_result = spark.sql(join_statement)

    # group by order key and get the associated partnames as a set
    result = join_result.groupBy(join_result['o.orderkey']).agg(functions.collect_set(join_result['p.name'])).withColumnRenamed("collect_set(name)", "part_names").orderBy(join_result['o.orderkey'], ascending=True).cache()
    result.registerTempTable('order_part')

    # Finally join orders table and order_part table
    final = spark.sql("SELECT a.*, b.part_names FROM orders a JOIN order_part b ON (a.orderkey = b.orderkey) order by a.orderkey asc")

    # Write it into cassandra table
    final.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=output_keyspace).save()


if __name__ == '__main__':
    keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    main(keyspace,output_keyspace)
