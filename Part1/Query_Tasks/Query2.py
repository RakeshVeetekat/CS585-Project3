import math
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

def check_duplicate(f, person):
    all_entries = f.readlines()
    for x in all_entries:
        if person.split(",")[0] in x:
            return True

    return False

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
            if infected_person != 'ID, x, y, age':
                for person in all_people:
                    if person != 'ID, x, y, age':
                        if close_contact(infected_person, person) and check_duplicate(f, person) == False:
                            f.write("P_ID: " + person.split(",")[0] + ", Infected-ID: " + infected_person.split(",")[0] + "\n")

    f.close()

    output = sc.textFile("output_rdd.txt")
    output.saveAsTextFile("results")

    # Stop Spark context
    sc.stop()

if __name__ == '__main__':
    main()