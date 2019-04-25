#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 6 (Question 1 - correlate_logs.py)

#  Submission Date: 17th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


#assert sys.version_info >= (3, 5)
# make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
from pyspark import SQLContext
from pyspark.sql.functions import lit
from pyspark import SparkContext, SparkConf
import re, string
import math
import sys

conf = SparkConf().setAppName('pyspark')
sc = SparkContext(conf=conf)
sqlc=SQLContext(sc)
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

# returns name,bytes,count
def words_once(line):
    updated_line = line_re.split(line)
    if len(updated_line) >= 4:
        if ((len(updated_line[1]) > 0) and (len(updated_line[4]) > 0)):
            return Row(updated_line[1],float(updated_line[4]))

def main(inputs):
    data_frame_schema = types.StructType([
    types.StructField('name', types.StringType(), False),
    types.StructField('bytes', types.FloatType(), False),])
    text = sc.textFile(inputs).repartition(8)
    input_values = text.map(words_once).filter(lambda x: x is not None)
    df=sqlc.createDataFrame(input_values, data_frame_schema)
    df = df.withColumn("count", lit(1)) #Add count column
    # For each name (i.e group by) get the sum of bytes and rename it as x and get sum of count and rename as y
    table = df.groupby('name').agg(functions.sum(df['bytes']),functions.count(df['count'])).withColumnRenamed("SUM(bytes)", "x").withColumnRenamed("COUNT(count)", "y")
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
    inputs = sys.argv[1]
    main(inputs)
