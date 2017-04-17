#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file1=sys.argv[1]
input_text_file2=sys.argv[2]

names=sc.textFile(input_text_file1).\
        filter(lambda x: "Starting Session " in x and " of user " in x).\
        map(lambda x: x.rsplit(None, 1)[-1].replace('.','')).\
        map(lambda x: (x,None)).\
        reduceByKey(lambda x,y: x).\
        map(lambda x: x[0])

names2=sc.textFile(input_text_file2).\
        filter(lambda x: "Starting Session " in x and " of user " in x).\
        map(lambda x: x.rsplit(None, 1)[-1].replace('.','')).\
        map(lambda x: (x,None)).\
        reduceByKey(lambda x,y: x).\
        map(lambda x: x[0])

print "Q3: unique user names"
print "+ "+input_text_file1+ " : "+ str(names.take(100))
print "+ "+input_text_file2+" : " + str(names2.take(100))