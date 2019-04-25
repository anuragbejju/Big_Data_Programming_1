#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 8 (Question 1 - correlate_logs_cassandra.py)

#  Submission Date: 2nd November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import lit
import sys,os
import string,math

def main(keyspace,table_name):

    # connecting to keyspace
    cluster_seeds = ['199.60.17.188', '199.60.17.216']

    # Creating spark session
    spark = SparkSession.builder.appName('correlate_logs_cassandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    # Creating DataFrame and passing values
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).load().cache()

    df = df.withColumn("count", lit(1)) #Add count column
    # For each name (i.e group by) get the sum of bytes and rename it as x and get sum of count and rename as y
    table = df.groupby('host').agg(functions.sum(df['bytes']),functions.count(df['count'])).withColumnRenamed("SUM(bytes)", "x").withColumnRenamed("COUNT(count)", "y")
    # Add a column with 1 in order to get a count later
    table1 = table.withColumn("one", lit(1))
    # Add a x square column to table
    table2 = table1.withColumn("x_square", table1['x']*table1['x'])
    # Add a y square column to table
    table5 = table2.withColumn("y_square", table2['y']*table2['y'])
    # Add a x*y column to table
    table3 = table5.withColumn("x_y", table5['x']*table5['y'])
    #Create a final table with sum of 6 columns , one, x, y, x^2, y^2 and x*6
    table4 = table3.agg(functions.sum(table3['one']),functions.sum(table3['x']),functions.sum(table3['y']),functions.sum(table3['x_square']),functions.sum(table3['y_square']),functions.sum(table3['x_y']))
    sumed_values = table4.collect()
    sum_n = sumed_values[0][0] #Count
    sum_x = sumed_values[0][1] #Sum of x
    sum_y = sumed_values[0][2] #Sum of y
    sum_x_2 = sumed_values[0][3]  #Sum of x^2
    sum_y_2 = sumed_values[0][4]  #Sum of y^2
    sum_x_y = sumed_values[0][5]  #Sum of x*y

    temp_val = (sum_n*sum_x_y) - (sum_x*sum_y)
    temp_val2 = math.sqrt((sum_n*sum_x_2) - (sum_x*sum_x))
    temp_val3 = math.sqrt((sum_n*sum_y_2) - (sum_y*sum_y))
    r = (temp_val)/((temp_val2)*(temp_val3)) #Calculate r
    print ('r = '+ "{0:.6f}".format(r))
    print ('r^2 = '+ "{0:.6f}".format(r*r)) #calculate r^2

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main(keyspace,table_name)
