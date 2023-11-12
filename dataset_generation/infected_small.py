# generate a file infected-small.csv with only the infected people
# holds the same schema as people-large.csv
def main():
    src = "../datasets/people-large-infected.csv"
    dst = "../datasets/infected-small.csv"
    new_lines = []

    with open(src, "r") as f:
        for line in f.readlines():
            arr = line.split(",")
            if "true" in line:
                new_lines.append(line.replace(",true", ""))
            
            if "ID" in line:
                new_lines.append(line.replace(",INFECTED", "")) 

    f.close()

    with open(dst, "w") as f:
        for line in new_lines:
            f.write(line)

    f.close()


if __name__ == '__main__':
    main()