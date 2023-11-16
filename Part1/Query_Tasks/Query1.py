import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

def close_contact(personA, personB):
    # compute euclidean distance here
    pass


def main():
    # Configure Spark
    conf = SparkConf().setAppName("Query1")
    sc = SparkContext(conf=conf)

    # Read input file
    rddAllPeople = sc.textFile("../DataCreation/datasets/people-large.csv")
    rddInfected = sc.textFile("../DataCreation/datasets/infected-small.csv")

    all_people = rddAllPeople.collect()

    with open("output_rdd.txt", "w") as f:
        for infected_person in rddInfected.collect():
            for person in all_people:
                if close_contact(infected_person, person):
                    f.write("infected person, close contact")

    f.close()

    output = sc.textFile("output_rdd.txt")
    output.saveAsTextFile("results")

    # Stop Spark context
    sc.stop()

if __name__ == '__main__':
    main()