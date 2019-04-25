#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 2 (Question 3 - wikipedia_popular.py)

#  Submission Date: 17th September 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


#  Code Synopsis

#  words_in_line function - Inputs line and split it after spaces. It also type casts the page request count to int.
#  find_max_value function - find the max value.
#  tab_separated function: formats the value as the requirement of problem statement.

#  Program Flow: One line is split based on single space and the values are placed in (String, [int , string]) format
#  Then the data is reduced and the max value for the page count is found
#  Then the values are sorted and outputed to a file in required format

from pyspark import SparkConf, SparkContext
import sys
import operator

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular Python')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_in_line(line):
    one_line = line.split()
    yield (one_line[0],[int(one_line[3]),one_line[2]])

def find_max_value(l,m):
    return max(l,m)

def tab_separated(one_entry):
    return "%s\t(%s , '%s')" % (one_entry[0][:11], one_entry[1][0], one_entry[1][1])

data = sc.textFile(inputs)
words = data.flatMap(words_in_line)
reduce_step = words.reduceByKey(find_max_value)
sort_step = reduce_step.sortByKey()
sort_step.map(tab_separated).coalesce(1).saveAsTextFile(outputs)
