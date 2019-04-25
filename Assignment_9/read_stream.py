#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 9 (Question 1 - read_stream.py)

#  Submission Date: 11th November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import split
spark = SparkSession.builder.appName('example code').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(topic):

    # Read stream messages
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()

    # Split the input value with the first value being x and second value being y
    values = messages.selectExpr("split(value,' ')[0] as x","split(value,' ')[1] as y")

    # Add a x square column to table
    values = values.withColumn("x_square", values['x']*values['x'])

    # Add a x*y column to table
    values = values.withColumn("x_y", values['x']*values['y'])

    # Have a final table with sum of columns x, y, x*y, x^2 and total count n
    values.createOrReplaceTempView("values_table")
    updated_values = spark.sql("Select SUM(x) as sum_x, SUM(y) as sum_y, SUM(x_y) as sum_x_y, SUM(x_square) as sum_x_2, COUNT(*) as n from values_table")

    # Do beta/ slope calculation
    beta = (updated_values['sum_x_y']-((updated_values['sum_x']*updated_values['sum_y'])/updated_values['n']))/(updated_values['sum_x_2']-((updated_values['sum_x']*updated_values['sum_x'])/updated_values['n']))
    updated_values = updated_values.withColumn("slope_beta", beta)

    # Do alpha / intercept calculation
    alpha = (updated_values['sum_y']/updated_values['n'])-(updated_values['slope_beta']*(updated_values['sum_x']/updated_values['n']))
    updated_values = updated_values.withColumn("intercept_aplha", alpha)

    # Select only alpha , beta values
    values = updated_values.select(updated_values['intercept_aplha'],updated_values['slope_beta'])

    # Print out alpha and beta values
    stream = values.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)

if __name__ == "__main__":
    topic = sys.argv[1]
    main(topic)
