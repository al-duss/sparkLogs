#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file1=sys.argv[1]
input_text_file2=sys.argv[2]

count=sc.textFile(input_text_file1).\
        filter(lambda x: "error" in x.lower()).\
        map(lambda x: x.rsplit(None, 1)[-1].replace('.','')).\
        count()

count2=sc.textFile(input_text_file2).\
        filter(lambda x: "error" in x.lower()).\
        map(lambda x: x.rsplit(None, 1)[-1].replace('.','')).\
        count()

print "Q5: number of errors"
print "+ "+input_text_file1+ " : "+ str(count)
print "+ "+input_text_file2+" : " + str(count2)