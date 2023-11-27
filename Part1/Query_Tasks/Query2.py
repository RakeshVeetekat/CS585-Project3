import math
import os

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

# remove the duplicates from the output
def check_duplicate(f, person):
    if os.stat("output_rdd_query2.txt").st_size != 0:
        all_entries = f.readlines()
    else:
        return False

    for x in all_entries:
        if person.split(",")[0] in x:
            return True

    return False


#check to see if a person is a close contact with euclidean distance
def close_contact(personA, personB):
    personA_x = int(personA.split(",")[1])
    personA_y = int(personA.split(",")[2])
    personB_x = int(personB.split(",")[1])
    personB_y = int(personB.split(",")[2])
    if math.dist([personA_x,personA_y],[personB_x,personB_y]) <= 6:
        return True
    else:
        return False
    

#main function of the program
def main():
    # Configure Spark
    conf = SparkConf().setAppName("Query2")
    sc = SparkContext(conf=conf)

    # Read input file
    rddAllPeople = sc.textFile("../DataCreation/datasets/people-large.csv")
    rddInfected = sc.textFile("../DataCreation/datasets/infected-small.csv")

    all_people = rddAllPeople.collect()

    with open("output_rdd_query2.txt", "w") as f:
        for infected_person in rddInfected.collect():
            if infected_person != 'ID, x, y, age':
                for person in all_people:
                    if person != 'ID, x, y, age':
                        if close_contact(infected_person, person) and check_duplicate(f, person) == False:
                            f.write("P_ID: " + person.split(",")[0] + ", Infected-ID: " + infected_person.split(",")[0] + "\n")

    f.close()

    output = sc.textFile("output_rdd_query2.txt")
    output.saveAsTextFile("results_query2")

    # Stop Spark context
    sc.stop()


if __name__ == '__main__':
    main()