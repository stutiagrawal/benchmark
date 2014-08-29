import os
import sys
def download_data(dirname, filename):
    
    fp = open(filename, "r")
    line = fp.readline()
    """
    line = line.split("\t")
    for i in xrange(len(line)):
        print i, line[i]
    """
    for line in fp:
        line = line.split("\t")
        uuid = line[16]
        if(not(os.path.isdir(os.path.join(dirname, uuid)))):
            os.system('gtdownload -v -c ~/keys/cghub.key -d %s' %(uuid))

download_data(sys.argv[1], sys.argv[2])
