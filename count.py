import os
import sys
import fileinput
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/2019/'
files= os.listdir(path)
tc = 0
for file in files:
    if '.bz2' in file:
        count = 0
        revert = 0
        inputs = fileinput.FileInput(path+file, openhook=fileinput.hook_compressed)
        for line in inputs:
            line = line.decode("utf-8").rstrip()
            count += 1
            if 'rollback' in line:
                revert += 1
         
        print(file, count, revert)
        tc += count   
print(tc)

