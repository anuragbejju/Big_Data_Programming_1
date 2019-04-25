#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 9 (Question 3 - weather_train.py)

#  Submission Date: 11th November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker

import sys
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+


from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import dayofyear
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def train_model(model_file, inputs):

    # Read the CSV File
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)

    # Split the dataset. Make 75% as training set and the remaining 25% as validation set
    train, validation = test_tmax.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # SQL Query having no yesterday tmax
    query_without_yesterday = "SELECT DAYOFYEAR(today.date) as day_of_year, today.longitude,today.latitude,today.elevation,today.tmax,today.station FROM __THIS__ as today"
    sqlTrans_without_yesterday = SQLTransformer(statement=query_without_yesterday)
    # SQL Query having yesterday tmax
    query_with_yesterday = "SELECT DAYOFYEAR(today.date) as day_of_year,today.longitude,today.latitude,today.elevation,yesterday.tmax AS yesterday_tmax,today.tmax,today.station FROM __THIS__ as today  INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
    sqlTrans_with_yesterday = SQLTransformer(statement=query_with_yesterday)

    # Feature assembler not considering yesterday_tmax as a feature
    feature_assembler_without_yesterday = VectorAssembler(inputCols=["latitude", "longitude", "elevation","day_of_year"], outputCol="features")
    # Feature assembler considering yesterday_tmax as a feature
    feature_assembler_with_yesterday= VectorAssembler(inputCols=["latitude", "longitude", "elevation","day_of_year","yesterday_tmax"], outputCol="features")

    # Using GBTRegressor
    word_indexer = StringIndexer(inputCol="station", outputCol="label", handleInvalid='error')
    estimator = GBTRegressor(featuresCol = 'features', labelCol = 'tmax', maxIter = 100)

    # Create pipelines for models being made with and without yesterday_tmax feature
    feature_pipeline_without_yesterday = Pipeline(stages=[sqlTrans_without_yesterday,feature_assembler_without_yesterday,word_indexer,estimator])
    feature_pipeline_with_yesterday = Pipeline(stages=[sqlTrans_with_yesterday,feature_assembler_with_yesterday,word_indexer,estimator])

    # Training models with and without yesterday_tmax feature
    model_without_yesterday = feature_pipeline_without_yesterday.fit(train)
    model_with_yesterday = feature_pipeline_with_yesterday.fit(train)

    # use the model to make predictions
    predictions_without_yesterday = model_without_yesterday.transform(validation)
    predictions_with_yesterday = model_with_yesterday.transform(validation)

    # evaluate the predictions without yesterday_tmax as a feature
    r2_evaluator_without_yesterday = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='r2')
    r2_without_yesterday = r2_evaluator_without_yesterday.evaluate(predictions_without_yesterday)

    rmse_evaluator_without_yesterday = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='rmse')
    rmse_without_yesterday = rmse_evaluator_without_yesterday.evaluate(predictions_without_yesterday)

    # evaluate the predictions with yesterday_tmax as a feature
    r2_evaluator_with_yesterday = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='r2')
    r2_with_yesterday = r2_evaluator_with_yesterday.evaluate(predictions_with_yesterday)

    rmse_evaluator_with_yesterday = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='rmse')
    rmse_with_yesterday = rmse_evaluator_with_yesterday.evaluate(predictions_with_yesterday)

    # r^2 and rmse values for models not considering yesterday_tmax as a feature
    print('r2 without yesterday tmax as a feature =', r2_without_yesterday)
    print('rmse without yesterday tmax as a feature =', rmse_without_yesterday)

    # r^2 and rmse values for models considering yesterday_tmax as a feature
    print('r2 with yesterday tmax as a feature =', r2_with_yesterday)
    print('rmse with yesterday tmax as a feature =', rmse_with_yesterday)

    # save model considering yesterday_tmax as a feature
    model_with_yesterday.write().overwrite().save(model_file)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    #print(model_with_yesterday.stages[-1].featureImportances)
    #print(model_without_yesterday.stages[-1].featureImportances)


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]

    train_model(model_file, inputs)
