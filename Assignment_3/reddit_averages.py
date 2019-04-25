#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 3 (Question 1 - reddit_averages.py)

#  Submission Date: 26th September 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker



from pyspark import SparkConf, SparkContext
import sys
import operator
import json


def words_in_line(line):
    yield (line['subreddit'],[int(line['score']),1]) #get the required values

def sum_value(l,m):
    return (l[0]+m[0],l[1]+m[1]) #Sum(score and frequency individually)

def format_value(one_entry):
    return (one_entry[0], float(one_entry[1][0])/float(one_entry[1][1])) #format it

def main(inputs, output):
    data = sc.textFile(inputs)
    words = data.map(json.loads)
    map_out = words.flatMap(words_in_line)
    reduce_step = map_out.reduceByKey(sum_value)
    sort_step = reduce_step.sortByKey()
    formatted_text = sort_step.map(format_value)
    json_text = formatted_text.map(json.dumps)
    json_text.coalesce(1).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Averages Python')
    sc = SparkContext(conf=conf)
#    assert sc.version >= '2.3'  make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
