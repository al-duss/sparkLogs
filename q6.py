#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

word = sys.argv[1]
word2= sys.argv[2]
input_text_file1=sys.argv[1]
input_text_file2=sys.argv[2]

errors=sc.textFile(input_text_file1).\
        filter(lambda x: "error" in x.lower()).\
        map(lambda x: x.split(word, 1)[1]).\
        map(lambda x: (x,1)).\
        reduceByKey(lambda x,y: x+y).\
        map(lambda x: (x[1],x[0])).\
        sortByKey(False)


# errors2=sc.textFile(input_text_file2).\
#         filter(lambda x: "error" in x.lower()).\
#         map(lambda x: x.split(word2, 1)[1]).\
#         map(lambda x: (x,1)).\
#         reduceByKey(lambda x,y: x+y).\
#         map(lambda x: (x[1],x[0])).\
#         sortByKey(False)

list1 = str(errors.take(5)).split("), (")
list2 = str(errors.take(5)).split("), (")
print "Q6: number of errors"
print "+ "+input_text_file1+ " : "
print "- "+list1[0].replace("[(","")
print "- "+list1[1]
print "- "+list1[2]
print "- "+list1[3]
print "- "+list1[4].replace(")]","")
# print "+ "+input_text_file1+ " : "
# print "- "+list2[0].replace("[(","")
# print "- "+list2[1]
# print "- "+list2[2]
# print "- "+list2[3]
# print "- "+list2[4].replace(")]","")
print "Weird error with odyssey where Odyssey is index out of bounds, therefore does not work."