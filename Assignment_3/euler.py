#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 3 (Question 3 - euler.py)

#  Submission Date: 17th September 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker

# Input Value; 1000000000
# Lowest Run Time = 3m12.936s at 8 partitions


from pyspark import SparkConf, SparkContext
import sys
import operator
import random


def euler_calc(a):
    total = 0
    random.seed()
    for i in range(a):
        sum = 0.0
        while sum < 1:
            sum = sum + random.random()
            total = total +1
    return (total)


def main(inputs):
    data = int(inputs)
    rdd_numbers= sc.parallelize([data//8]*8)
    map_output = rdd_numbers.map(euler_calc)
    red_output = map_output.reduce(operator.add)
    print (red_output/data)

if __name__ == '__main__':
    inputs = sys.argv[1]
    conf = SparkConf().setAppName('Euler Python')
    sc = SparkContext(conf=conf)
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    main(inputs)
