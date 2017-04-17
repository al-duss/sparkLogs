#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("q1").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file1=sys.argv[1]
input_text_file2=sys.argv[2]

counts=sc.textFile(input_text_file1).\
        filter(lambda x: len(x)>0).\
        count()

counts2=sc.textFile(input_text_file2).\
        filter(lambda x: len(x)>0).\
        count()

print "Q1: line counts"
print "+ "+input_text_file1+ " : "+ str(counts)
print "+ "+input_text_file2+" : " + str(counts2)