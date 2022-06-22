from unittest import result
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("data/Dataset100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
results = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
    print("xd)
