#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 5 (Question 4 - temp_range_sql.py)

#  Submission Date: 10th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import abs
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName('Weather_ETl').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+



def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("weather_table") #Read the input files into a DataFrame
    weather_filter_min = spark.sql("Select date, station, min(value) as min_value from weather_table where qflag is NUll and observation = 'TMIN' group by date,station")
    weather_filter_max = spark.sql("Select date, station, max(value) as max_value from weather_table where qflag is NUll and observation = 'TMAX' group by date,station")
    weather_filter_min.createOrReplaceTempView("min_table")
    weather_filter_max.createOrReplaceTempView("max_table")
    weather_join = spark.sql("Select mi.date,mi.station,Abs((ma.max_value-mi.min_value)/10) as range from min_table  as mi inner join max_table as ma on mi.date == ma.date and mi.station == ma.station")
    weather_join.createOrReplaceTempView("temporary_table")
    weather_grouped = spark.sql("Select date, max(range) as max_range from temporary_table group by date")
    weather_grouped.createOrReplaceTempView("temporary_table1")
    weather_output = spark.sql("Select tt.date,tt.station, tt.range from temporary_table  as tt inner join temporary_table1 as tt1 on tt.date == tt1.date where tt.range == tt1.max_range order by tt.date, tt.station ASC ")
    weather_output.write.csv(output, mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
