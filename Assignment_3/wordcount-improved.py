#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 3 (Question 2 - wordcount-improved.py)

#  Submission Date: 19th September 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


#With 8 partitions it ran for 2:32 min and the values are
#real	2m32.331s

from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

def words_once(line):
    updated_line = wordsep.split(line)
    for w in updated_line:
        if len(w) >0:
            yield (w.lower(), 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    text = sc.textFile(inputs).repartition(8)
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(operator.add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('word count')
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
