#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 5 (Question 3 - temp_range.py)

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

    weather = spark.read.csv(inputs, schema=observation_schema) #Read the input files into a DataFrame
    t_min = weather.filter((weather.qflag.isNull())&(weather.observation=='TMIN')) #the field qflag (quality flag) is null, the station starts with 'CA' and the observation is 'TMAX'
    t_max = weather.filter((weather.qflag.isNull())&(weather.observation=='TMAX'))
    t_min_selected = t_min.select('date','station','value')
    t_max_selected = t_max.select('date','station','value')
    t_min_group = t_min_selected.groupby('date','station').agg(functions.min(t_min_selected['value'])).withColumnRenamed("MIN(value)", "min_count")
    t_max_group = t_max_selected.groupby('date','station').agg(functions.max(t_max_selected['value'])).withColumnRenamed("MAX(value)", "max_count")
    weather_joined = t_min_group.join(broadcast(t_max_group), ((t_max_group['date'] == t_min_group['date'])&((t_max_group['station'] == t_min_group['station'])))).drop(t_min_group.date).drop(t_min_group.station)
    weather_joined_newcol = weather_joined.withColumn("range", abs((weather_joined.min_count - weather_joined.max_count)/10)) # Divide the temperature by 10 so it's actually in °C, and call the resulting column tmax.
    weather_joined_group = weather_joined_newcol.select('date', 'range').groupby('date').agg(functions.max(weather_joined_newcol['range']))
    weather_joined_two = weather_joined_newcol.join(broadcast(weather_joined_group), (weather_joined_group['date'] == weather_joined_newcol['date'])).drop(weather_joined_newcol.date)
    weather_joined_filter = weather_joined_two.filter(weather_joined_two['range'] == weather_joined_two['max(range)']).sort('date','station', ascending=True)
    final_output = weather_joined_filter.select('date','station','range')
    final_output.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
