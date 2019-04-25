#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 4 (Question 3 - relative_score_bcast.py)

#  Submission Date: 5th October 2018
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

def clean_data(one_entry,aver):
    ref = one_entry[0]
    avg_val = float(aver.value[ref])
    if avg_val > 0:
        return ( float(one_entry[1][0])/avg_val,one_entry[1][1])

def main(inputs, output):
    data = sc.textFile(inputs).repartition(8)
    commentdata = data.map(json.loads).cache()
    map_out = commentdata.flatMap(words_in_line)
    reduce_step = map_out.reduceByKey(sum_value)
    formatted_text = reduce_step.map(format_value) #RDD of pairs (subreddit, average score)
    commentbysub = commentdata.map(lambda c: (c['subreddit'], (c['score'], c['author']))) #RDD of pairs (subreddit, (score, author))
    dict_avgvalues = dict(formatted_text.collect()) #Used collect as the data size is smalll
    aver = sc.broadcast(dict_avgvalues)
    clean_val = commentbysub.map(lambda x : clean_data(x,aver)) #RDD of pairs (comment['score']/average and comment['author'])
    sort_data = clean_val.sortByKey(False)#sort descending based on relative score
    json_text = sort_data.map(json.dumps)
    json_text.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('Relative Score Broadcast')
    sc = SparkContext(conf=conf)
#    assert sc.version >= '2.3'  make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
