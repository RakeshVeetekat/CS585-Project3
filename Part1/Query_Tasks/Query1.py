import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

# Configure Spark
conf = SparkConf().setAppName("Query1")
sc = SparkContext(conf=conf)

# Read input file
rddAllPeople = sc.textFile("../DataCreation/datasets/people-large.csv")
rddInfected = sc.textFile("../DataCreation/datasets/infected-small.csv")

# Filter for all the close contacts to infected people
def f(x):
    print(x)
    line = x.split(",")
    infectedID = line[0]
    infectedX = line[1]
    infectedY = line[2]

output = rddInfected.map(f)

# Save results to a file
output.saveAsTextFile("output")

# Stop Spark context
sc.stop()