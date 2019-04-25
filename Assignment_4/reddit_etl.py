#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 4 (Question 1 - redditetl.py)

#  Submission Date: 5th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker

#execution time for processing reddit-4 input file without cache -1m35.200s
#execution time for processing reddit-4 input file with cache - 0m54.818s

from pyspark import SparkConf, SparkContext
import sys
import operator
import json

def words_in_line(line):
    #Keep only the fields subreddit, score, and author as well as the data is from subreddits that contain an 'e' in their name.
    if 'e' in (line['subreddit']).lower():
        yield (line['subreddit'],int(line['score']),line['author'])

def main(inputs, output):
    data = sc.textFile(inputs).repartition(8)
    words = data.map(json.loads)
    map_out = words.flatMap(words_in_line).cache() #Use cache as this RDD is used twice
    pos = map_out.filter(lambda x : x[1] > 0) #filter all positive rows
    neg = map_out.filter(lambda x : x[1] <= 0) #filter all negative and zero rows
    pos.map(json.dumps).saveAsTextFile(output + '/positive')
    neg.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit ETL')
    sc = SparkContext(conf=conf)
    assert sys.version_info >= (3, 5)  #make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
