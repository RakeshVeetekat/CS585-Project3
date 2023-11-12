import shutil
import random


# generate a boolean for if a person is infected or not
def is_infected():
    number = random.randint(0, 100)
    if number % 2 == 0:
        return "true"
    
    return "false"

# copy the people-large.csv and add a new aattribute for INFECTED
def main():
    src = "../datasets/people-large.csv"
    dst = "../datasets/people-large-infected.csv"

    new_lines = []

    with open(src, "r") as f:
        for line in f.readlines():
            line = line.strip("\n")

            if line.startswith("ID"):
                line = line + "," + "INFECTED" + "\n"
                new_lines.append(line)
                continue

            line = line + "," + is_infected() + "\n"
            new_lines.append(line)
        
    f.close()

    with open(dst, "w") as f:
        for line in new_lines:
            f.write(line)

    f.close()


if __name__ == '__main__':
    main()