#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 6 (Question 2 - shortest_path.py)

#  Submission Date: 17th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark import SQLContext
import re, string
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('Shortest Path')
sc = SparkContext(conf=conf)

#splits the input text to keys and values
def get_keys_values(line):
    updated_line = re.findall(r"[\w']+", line)
    updated_line = list(map(int,updated_line))
    yield (updated_line[0], updated_line[1:])

def get_only_values(value):
    return value

#re-maps values in (node, (source, distance)) format
def map_values(value):
    return (value[1][0], (value[0], value[1][1][1]+1))

#reduce the path values based on shortest distance
def reduce_function(x,y):
    if x[1] > y[1]:
        return y
    else:
        return x

#formats values in as per the specification
def format_values(value):
    yield ('node '+str(value[0])+': source '+str(value[1][0])+' , distance '+str(value[1][1])+'')

def main(inputs,output,start,end):
    start_node = int(start) # Starting node
    end_node = int(end) # Ending node
    read_file = sc.textFile(inputs+'/links-simple-sorted.txt') # Input the specified file links-simple-sorted.txt
    input_values = read_file.flatMap(get_keys_values) #get the input values in tuples
    get_edges = input_values.flatMapValues(get_only_values).cache() # get the edges list
    path = sc.parallelize([(start_node,('_',0))]) # initialize path with the no source value
    for i in range(6): # Since can be changed based on our requirement
        # First filter out the path with the current distance i and then join it with the edges list
        Edges_joined_with_Path = get_edges.join(path.filter(lambda filter_value : filter_value[1][1] == i))
        # Then map and get the values in (node, (source, distance))
        Temp_Path = Edges_joined_with_Path.map(map_values)
        # Then reduce it by key by choosing the tuple with least distance
        path = path.union(Temp_Path).reduceByKey(reduce_function)
        # Format the path values as per the requirement
        path_formated = path.flatMap(format_values)
        #each path should be put in its iteration file
        path_formated.coalesce(1).saveAsTextFile(output + '/iter-' + str(i+1))
        #This the condition to break where we check if the end node has been reached by the path finder
        if end_node in Temp_Path.keys().collect():
            break
    #We cache the path as we would be doing a lot of lookup to the rdd
    final_path_values = path.cache()
    #Create a route list by initializing the end node in it
    final_route = [end_node]
    #we use a while loop until the start node is not reached
    while end_node!= start_node and end_node != '':
        searched_value = final_path_values.lookup(end_node) #check for the value in our path RDD
        if searched_value[0][0] =='':
            break
        else:
            end_node = searched_value[0][0] #assign the source node value to end_node
            final_route.append(end_node) #append the above computed value to our final route

    final_route_reversed = sc.parallelize(final_route[::-1]) #Since its like a stack, we reverse it to start from Source -> Destination
    final_route_reversed.coalesce(1).saveAsTextFile(output + '/path') #Output the path in a file

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    main(inputs,output,start,end)
