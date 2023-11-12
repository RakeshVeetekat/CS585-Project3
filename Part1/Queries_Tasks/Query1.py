import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf.setAppName("Query1")
sc = SparkContext(conf=conf)

