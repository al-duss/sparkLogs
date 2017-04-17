#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file1=sys.argv[1]
input_text_file2=sys.argv[2]

counts=sc.textFile(input_text_file1).\
        filter(lambda x: "Starting Session " in x and " of user achille" in x).\
        count()

counts2=sc.textFile(input_text_file2).\
        filter(lambda x: "Starting Session " in x and " of user achille" in x).\
        count()

print "Q2: sessions of user 'achille'"
print "+ "+input_text_file1+ " : "+ str(counts)
print "+ "+input_text_file2+" : " + str(counts2)