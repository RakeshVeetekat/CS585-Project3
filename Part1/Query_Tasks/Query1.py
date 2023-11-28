import math
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

def close_contact(personA, personB):
    # compute euclidean distance here
    personA_x = int(personA.split(",")[1])
    personA_y = int(personA.split(",")[2])
    personB_x = int(personB.split(",")[1])
    personB_y = int(personB.split(",")[2])
    if math.dist([personA_x,personA_y],[personB_x,personB_y]) <= 6:
        return True
    else:
        return False
    pass


def main():
    # Configure Spark
    conf = SparkConf().setAppName("Query1")
    sc = SparkContext(conf=conf)

    # Read input file
    rddAllPeople = sc.textFile("../DataCreation/datasets/people-large.csv")
    rddInfected = sc.textFile("../DataCreation/datasets/infected-small.csv")

    all_people = rddAllPeople.collect()

    with open("output_rdd_query1.txt", "w") as f:
        for infected_person in rddInfected.collect():
            if infected_person != 'ID, x, y, age':
                for person in all_people:
                    if person != 'ID, x, y, age':
                        if close_contact(infected_person, person) and person.split(",")[0] != infected_person.split(",")[0]:
                            f.write("P_ID: " + person.split(",")[0] + ", Infected-ID: " + infected_person.split(",")[0] + "\n")

    f.close()

    output = sc.textFile("output_rdd_query1.txt")
    output.saveAsTextFile("results_query1")

    # Stop Spark context
    sc.stop()

if __name__ == '__main__':
    main()