#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 4 (Question 4 - Weather_ETl.py)

#  Submission Date: 5th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker



import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
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
    op = weather.filter((weather.qflag.isNull())&(weather.station.startswith('CA'))&(weather.observation=='TMAX')) #the field qflag (quality flag) is null, the station starts with 'CA' and the observation is 'TMAX'
    op = op.withColumn("tmax", (weather.value)/10) # Divide the temperature by 10 so it's actually in °C, and call the resulting column tmax.
    op = op.select('station', 'date', 'tmax') #Keep only the columns station, date, and tmax.
    op.write.json(output, compression='gzip', mode='overwrite') # Write the result as a directory of JSON files GZIP compressed


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
