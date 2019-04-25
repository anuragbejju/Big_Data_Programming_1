#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 9 (Question 2 - colour_predict.py)

#  Submission Date: 11th November 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker

import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert spark.version >= '2.3' # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def main(inputs):

    # Read the CSV File
    df = spark.read.csv(inputs, schema=colour_schema)

    # Total label count
    label_num = df.select('word').distinct().count()

    # Split the dataset. Make 75% as training set and the remaining 25% as validation set
    train, validation = df.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # Creating pipeline
    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    word_indexer = StringIndexer(inputCol="word", outputCol="label", handleInvalid="error")
    classifier_mpc = MultilayerPerceptronClassifier(layers=[3, 250, label_num])

    # Transformer for the lab pipeline
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=["labL", "labA", "labB"], outputCol="features")

    # TODO: create a pipeline to predict RGB colours -> word; train and evaluate.

    #  pipeline to predict RGB colours
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier_mpc])
    lab_pipeline = Pipeline(stages=[sqlTrans,lab_assembler,word_indexer,classifier_mpc])

    # Train the model
    rgb_model = rgb_pipeline.fit(train)
    lab_model = lab_pipeline.fit(train)

    # Transform the validation set
    predictions_rgb = rgb_model.transform(validation)
    predictions_lab = lab_model.transform(validation)

    # TODO: create an evaluator and score the validation data

    # Create a Multiclass Classification Evaluator
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

    # Evaluate it on validation data
    score_rgb = evaluator.evaluate(predictions_rgb)
    score_lab = evaluator.evaluate(predictions_lab)

    plot_predictions(rgb_model, 'RGB', labelCol='word')
    plot_predictions(lab_model, 'LAB', labelCol='word')

    # Print the validation scores
    print('Validation score for RGB model: %g' % (score_rgb, ))
    print('Validation score for LAB model: %g' % (score_lab, ))


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
