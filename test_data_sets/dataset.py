import sys
def create_dataset(filename, num_datasets=10):
    fp = open(filename, "r")
    header = fp.readline()
    count = 0
    i = 0
    for line in fp:
        if(count % 10 == 0):
            i += 1
            new_file = open("dataset_%d" %i, "w")
            new_file.write(header)
        new_file.write(line)
        count += 1
        if(count % 10 == 0):
            new_file.close()

create_dataset(sys.argv[1])
