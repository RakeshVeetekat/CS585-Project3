import os
import random

# generate x, y coordinate pair
def generate_coordinates():
    return [str(random.randint(0, 10000)), str(random.randint(0, 10000))]

# generate a random age attribute
def generate_age():
    return str(random.randint(1, 100))

# generate data for the people large dataset
# ARG size: the size of the dataset file 
def generate_data(size):
    id = 0
    filename = "../datasets/people-large.csv"
    with open(filename, "w+") as f:
        while(id < size):
            if id == 0:
                f.write("ID, x, y, age\n")
            arr = generate_coordinates()
            line = str(id) + "," + arr[0] + "," + arr[1] + "," + generate_age() + "\n"
            f.write(line)
            id += 1

    f.close()

def main():
    generate_data(1900000)

if __name__ == '__main__':
    main()