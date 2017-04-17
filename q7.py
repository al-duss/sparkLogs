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

n1 = names.take(100)
n2 = names2.take(100)
both = []
for n in n2:
        if n in n1:
                both.append(n)

print "Q7: users who started a session on both hosts, i.e, on exactly 2 hosts."
print "+ "+str(both)