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
    conf = SparkConf().setAppName("Query3")
    sc = SparkContext(conf=conf)

    # Read input file
    rdd1 = sc.textFile("../DataCreation/datasets/people-large-infected.csv")
    rdd2 = sc.textFile("../DataCreation/datasets/people-large-infected.csv")

    people_first_loop = rdd1.collect()

    # If the current person is infected, loop through rdd2 to determine number of close contacts
    with open("output_rdd_query3.txt", "w") as f:
        for current_person in people_first_loop:
            closeContactCount = 0
            # If the current person is infected, then loop through everyone to find close contacts
            if current_person != 'ID, x, y, age,INFECTED' and current_person.split(",")[4] == 'true':
                for possible_close_contacts in rdd2.collect():
                    if possible_close_contacts != 'ID, x, y, age,INFECTED':
                        if close_contact(current_person, possible_close_contacts) and current_person.split(",")[0] != possible_close_contacts.split(",")[0]:
                            closeContactCount+=1
                f.write("Infected-ID: " + current_person.split(",")[0] + ", Close-Contact-Count: " + str(closeContactCount) + "\n")

    f.close()

    output = sc.textFile("output_rdd_query3.txt")
    output.saveAsTextFile("results_query3")

    # Stop Spark context
    sc.stop()

if __name__ == '__main__':
    main()