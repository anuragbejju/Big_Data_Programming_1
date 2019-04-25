#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 9 (Question 4 - weather_tomorrow.py)

#  Submission Date: 11th November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


import sys
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert spark.version >= '2.3' # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import dayofyear
from pyspark import SQLContext
from pyspark import SparkContext, SparkConf
import datetime


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):

    # get the data
    data = [ ('SFU000001', datetime.date(2018, 11, 12), 49.2771,-122.9146,330.0,12.0),('SFU000001', datetime.date(2018, 11, 13), 49.2771,-122.9146,330.0,0.0)]
    test_tmax = spark.createDataFrame(data, tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(test_tmax)
    prediction_values = predictions.select("prediction").collect()

    # Print Predicted tmax value for 13th November 2018
    print ('Predicted tmax tomorrow:', str(prediction_values[0]['prediction']))

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
