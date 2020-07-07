import tqdm
import bz2
import os
import fileinput
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'


md_history_path = path + '2019/'
revert_info_path = path + '2019_revert/'

md_history_files = os.listdir(md_history_path)
action = {}
for file in md_history_files:
    print(md_history_files)
    if '.bz2' in file:
        outputfile = revert_info_path + file[:14]+'.tsv'
        mhf = fileinput.FileInput(md_history_path+file, openhook=fileinput.hook_compressed)
        print(file[:14])
        with  open(outputfile,'w') as output:
            par = tqdm.tqdm()
            for line in mhf:
                par.update(1)
                line = line.decode("utf-8").rstrip()
                tokens = line.split('\t')
                if 'mw-rollback' in tokens[-1]:
                    if tokens[-1] not in action:
                        action[tokens[-1]] = 1
                    else:
                        action[tokens[-1]] += 1
                    #print(tokens[-1] + '\n')


for act in action:
    print(act)
    print(action[act])

