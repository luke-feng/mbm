import tqdm
import bz2
import os
import fileinput
import random
import pandas as pd
import datetime
from statsmodels.formula.api import ols

path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
file = 'meta/2017_meta.csv'
metafile = path + file
def rev_user(metafile):
    par = tqdm.tqdm()
    with open(metafile, 'r')as meta, open(path+'rev_user_2017.csv','w')as out:
        for line in meta:
            par.update(1)
            line = line.rstrip()
            tokens = line.split(';')
            rev_id = tokens[3]
            user_id = tokens[6]
            if rev_id is not 'null' and 'None' not in user_id:
                out.write(rev_id+';'+user_id+'\n')
    return True

rev_user_file = path + 'rev_user_2017.csv'
def get_rev_user(rev_user_file):
    rev_user = {}
    par = tqdm.tqdm()
    with open(rev_user_file,'r') as ruf:
        for line in ruf:
            line = line.rstrip('\n')
            par.update(1)
            tokens = line.split(';')
            rev_id = tokens[0]
            user_id = tokens[1]
            rev_user[rev_id] = user_id
    return rev_user


rev_info_file = path + 'rev_info/2017.csv'
def get_reverted_rev(rev_info_file):
    with open(rev_info_file,'r') as rev_info, open(path+'revered_rev_user_2017.csv','w') as out:
        rev_user = get_rev_user(rev_user_file)
        par = tqdm.tqdm()
        for line in rev_info:
            par.update(1)
            line = line.rstrip('\n')
            tokens = line.split(';')
            parent_id = tokens[4]
            if parent_id in rev_user:
                user_id = rev_user[parent_id]
                if 'None' not in user_id:
                    out.write(parent_id+';'+user_id+'\n')
    return True

revered_rev_user1 = path + 'revered_rev_user.csv'
revered_rev_user2 = path + 'revered_rev_user_2018.csv'
revered_rev_user3 = path + 'revered_rev_user_2017.csv'

def count_dif_user(revered_rev_user1,revered_rev_user2,revered_rev_user3):
    users = {}
    with open(revered_rev_user1,'r') as infile1, open(revered_rev_user2,'r') as infile2, open(revered_rev_user3,'r') as infile3:
        for line in infile1:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
        for line in infile2:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
        for line in infile3:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
    print(len(users))
    return users
    


def sampling(revered_rev_user1,revered_rev_user2,revered_rev_user3):
    users = {}
    with open(revered_rev_user1,'r') as infile1, open(revered_rev_user2,'r') as infile2, open(revered_rev_user3,'r') as infile3, open(path+'sampling_rev.csv','w') as output:
        for line in infile1:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
        for line in infile2:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
        for line in infile3:
            line = line.rstrip()
            tokens = line.split(';')
            user_id = tokens[1]
            users[user_id] = tokens[0]
    
        for user in users:
            output.write(users[user]+';'+user+'\n')





md_history_path = path + '2019/'
revert_info_path = path + 'sampling_mw/'

def get_sample_mw_history():
    md_history_files = os.listdir(md_history_path)
    sampling_rev_file = path+'unrev_sample_rev.csv'
    sampling_rev = get_rev_user(sampling_rev_file)
    outputfile = revert_info_path + 'sample_unrev.csv'
    with  open(outputfile,'w') as output:
        for file in md_history_files:
            if '.bz2' in file:
                mhf = fileinput.FileInput(md_history_path+file, openhook=fileinput.hook_compressed)
                print(file[:14])
                print()
                par = tqdm.tqdm()
                for line in mhf:
                    par.update(1)
                    line = line.decode("utf-8").rstrip()
                    tokens = line.split('\t')
                    if  tokens[52] in sampling_rev:
                        output.write(line+'\n')
                        
def get_unrevert_action():
    md_history_files = os.listdir(md_history_path)
    not_revert_action_file = path+'not_revert_actions.csv'
    revert_users = count_dif_user(revered_rev_user1,revered_rev_user2,revered_rev_user3)
    unrevert_users = {}
    with open(not_revert_action_file,'w') as out:
        for file in md_history_files:
            if '.bz2' in file:
                mhf = fileinput.FileInput(md_history_path+file, openhook=fileinput.hook_compressed)
                print(file[:14])
                for line in mhf:
                    line = line.decode("utf-8").rstrip()
                    tokens = line.split('\t')
                    if ('revision'in tokens[1] or 'page' in tokens[1] ):
                        if 'roll' not in tokens[-1] and 'undo' not in tokens[-1] and 'revert' not in tokens[-1] and 'van' not in tokens[-1]:
                            if tokens[5] not in revert_users:
                                if 'null' not in tokens[52]:
                                    unrevert_users[tokens[5]] = tokens[52]

        for user in unrevert_users:
            out.write(unrevert_users[user]+';'+user+'\n')

def get_unrevert_sampling():
    unrevert_action_file = path+'not_revert_actions.csv'
    outputfile = path + 'unrev_sample_rev.csv'
    count = 0
    with open(unrevert_action_file,'r') as inputs, open(outputfile,'w') as outputs:
        for line in inputs:
            r = random.randint(1,9)
            if r%7 == 0 and count<150000:
                outputs.write(line)
                count += 1


def get_days_diff(t1,t2):
    a = datetime.datetime.strptime(t1,"%Y-%m-%d %H:%M:%S.%f")
    b = datetime.datetime.strptime(t2,"%Y-%m-%d %H:%M:%S.%f")
    c = b - a
    return c.days

def get_unre_2(inputfile, outputfile):
    count = 0
    with open(inputfile, 'r') as inp, open(outputfile, 'w') as outp:
        for line in inp:
            if '2018-05' in line or '2019-03' in line and count <= 10000:
                outp.write(line)
                count += 1

rev_sample_file = revert_info_path+'sample_rev_10000.csv'
unrev_sample_file = revert_info_path+'sample_unrev_10000.csv'
rev_metrics_file = revert_info_path + 'rev_res.csv'
unrev_metrics_file = revert_info_path + 'unrev_res.csv'

def get_users(inputfile):
    users = {}
    with open(inputfile, 'r') as inp:
        for line in inp:
            tokens = line.split('\t')
            user_id = tokens[5]
            user_name = tokens[6]
            users[user_id] = user_name
    return users

def get_basic(inputfile):
    users = {}
    with open(inputfile, 'r') as inp:
        for line in inp:
            info = []
            tokens = line.split('\t')
            user_id = tokens[5]
            user_name = tokens[6]
            revison_id = tokens[52]
            time = tokens[3]
            rev_count = tokens[21]
            info.append(user_name)
            info.append(revison_id)
            info.append(time)
            info.append(rev_count)
            users[user_id] = info 
    return users

def init_nor(sample_file, mwfile):
    user_basic_info = get_basic(sample_file)
    users = {}
    outputfile = rev_metrics_file
    par = tqdm.tqdm()
    with open(mwfile,'r') as next_month, open(outputfile, 'w') as output:
        for line in next_month:
            line = line.rstrip()
            par.update(1)
            tokens = line.split('\t')
            user_id = tokens[5]
            if 'revision' in tokens[1]:
                if user_id in user_basic_info:
                    base_time = user_basic_info[user_id][2]
                    if user_id not in users:
                        info = []
                        user_name = tokens[6]
                        revison_id = tokens[52]
                        time = tokens[3]
                        days = get_days_diff(time, base_time)
                        if days >= 0 and days <= 7:
                            rev_count = 1
                            word_account = int(tokens[58]) if 'null' not in tokens[58] else 0
                            info.append(user_name)
                            info.append(revison_id)
                            info.append(time)
                            info.append(rev_count)
                            info.append(word_account)
                            users[user_id] = info
                    else:
                        revison_id = tokens[52]
                        time = tokens[3]
                        days = get_days_diff(time, base_time)
                        if days >= 0 and days <= 7:
                            rev_count = tokens[21]
                            word_account = tokens[58]
                            users[user_id][1] = revison_id
                            users[user_id][2] = time
                            users[user_id][3] += 1
                            users[user_id][4] += (int(tokens[58]) if 'null' not in tokens[58] else 0)
        
        for user in user_basic_info:
            user_name = user_basic_info[user][0]
            revison_id = user_basic_info[user][1]
            time = user_basic_info[user][2]
            rev_count = int(user_basic_info[user][3])
            time1 = users[user][2]
            rev_count1 = int(users[user][3])
            word_account = users[user][4]
            days = get_days_diff(time, time1)
            if days != 0:
                rev_per_day = (rev_count1)/7
                word_per_day = word_account/7
            else:
                rev_per_day = (rev_count1)/7
                word_per_day = word_account/7
            line = [str(user), str(user_name), str(revison_id), str(time), str(rev_count), str(rev_per_day), str(word_per_day)]
            output.write(",".join(line) + '\n')


def num_of_revision(sample_file, mwfile, outputfile):
    user_basic_info = get_basic(sample_file)
    users = {}
    par = tqdm.tqdm()
    with open(mwfile,'r') as next_month, open(outputfile, 'w') as output:
        for line in next_month:
            line = line.rstrip()
            par.update(1)
            tokens = line.split('\t')
            user_id = tokens[5]
            if 'revision' in tokens[1]:
                if user_id in user_basic_info:
                    base_time = user_basic_info[user_id][2]
                    if user_id not in users:
                        info = []
                        user_name = tokens[6]
                        revison_id = tokens[52]
                        time = tokens[3]
                        days = get_days_diff(base_time, time)
                        rev_count1 = 0
                        rev_count2 = 0
                        rev_count3 = 0
                        rev_count4 = 0
                        word_account1 = 0
                        word_account2 = 0
                        word_account3 = 0
                        word_account4 = 0
                        if days > 0 and days <= 7:
                            rev_count1 = 1
                            word_account1 = int(tokens[58]) if 'null' not in tokens[58] else 0
                            info.append(days)
                            info.append(rev_count1)
                            info.append(word_account1)
                            info.append(rev_count2)
                            info.append(word_account2)
                            info.append(rev_count3)
                            info.append(word_account3)
                            info.append(rev_count4)
                            info.append(word_account4)
                            users[user_id] = info
                        if days > 7 and days <= 14:
                            rev_count2 = 1
                            word_account2 = int(tokens[58]) if 'null' not in tokens[58] else 0
                            info.append(days)
                            info.append(rev_count1)
                            info.append(word_account1)
                            info.append(rev_count2)
                            info.append(word_account2)
                            info.append(rev_count3)
                            info.append(word_account3)
                            info.append(rev_count4)
                            info.append(word_account4)
                            users[user_id] = info
                        if days > 14 and days <= 21:
                            rev_count3= 1
                            word_account3 = int(tokens[58]) if 'null' not in tokens[58] else 0
                            info.append(days)
                            info.append(rev_count1)
                            info.append(word_account1)
                            info.append(rev_count2)
                            info.append(word_account2)
                            info.append(rev_count3)
                            info.append(word_account3)
                            info.append(rev_count4)
                            info.append(word_account4)
                            users[user_id] = info
                        if days > 21 and days <= 28:
                            rev_count4 = 1
                            word_account4 = int(tokens[58]) if 'null' not in tokens[58] else 0
                            info.append(days)
                            info.append(rev_count1)
                            info.append(word_account1)
                            info.append(rev_count2)
                            info.append(word_account2)
                            info.append(rev_count3)
                            info.append(word_account3)
                            info.append(rev_count4)
                            info.append(word_account4)
                            users[user_id] = info
                    else:
                        revison_id = tokens[52]
                        time = tokens[3]
                        days = get_days_diff(base_time, time)
                        users[user_id][0] = days
                        if days > 0 and days <= 7:
                            users[user_id][1] += 1
                            users[user_id][2] += (int(tokens[58]) if 'null' not in tokens[58] else 0)
                        if days >7 and days <= 14:
                            users[user_id][3] += 1
                            users[user_id][4] += (int(tokens[58]) if 'null' not in tokens[58] else 0)
                        if days >14 and days <= 21:
                            users[user_id][5] += 1
                            users[user_id][6] += (int(tokens[58]) if 'null' not in tokens[58] else 0)
                        if days >21 and days <= 28:
                            users[user_id][7] += 1
                            users[user_id][8] += (int(tokens[58]) if 'null' not in tokens[58] else 0)
        
        for user in user_basic_info:
            user_name = user_basic_info[user][0]
            revison_id = user_basic_info[user][1]
            time = user_basic_info[user][2]
            if user in users:
                days = users[user][0]
                rev_count1 = int(users[user][1])
                word_account1 = users[user][2]
                rev_count2 = int(users[user][3])
                word_account2 = users[user][4]
                rev_count3 = int(users[user][5])
                word_account3 = users[user][6]
                rev_count4 = int(users[user][7])
                word_account4 = users[user][8]

                rev_per_day1 = (rev_count1)/7
                word_per_day1 = word_account1/7
                rev_per_day2 = (rev_count2)/7
                word_per_day2 = word_account2/7
                rev_per_day3 = (rev_count3)/7
                word_per_day3 = word_account3/7
                rev_per_day4 = (rev_count4)/7
                word_per_day4 = word_account4/7

                rev_per_day = (rev_per_day1 +rev_per_day2+rev_per_day3+rev_per_day4)/4
                word_per_day = (word_per_day1 +word_per_day2+word_per_day3+word_per_day4)/4
            else:
                rev_per_day1 = 0
                word_per_day1 = 0
                rev_per_day2 = 0
                word_per_day2 = 0
                rev_per_day3 = 0
                word_per_day3 = 0
                rev_per_day4 = 0
                word_per_day4 = 0

                rev_per_day = 0
                word_per_day = 0

            line = [str(user), str(user_name), str(revison_id), str(time),  str(rev_per_day1), str(word_per_day1), 
            str(rev_per_day2), str(word_per_day2),  str(rev_per_day3), str(word_per_day3),  str(rev_per_day4), str(word_per_day4),str(rev_per_day), str(word_per_day)]
            output.write(",".join(line) + '\n')

sample_file = rev_sample_file
mwfile = revert_info_path+'mw_file.tsv'
outputfile = revert_info_path+'unrev_result1.csv'

data_input_file = path + 'input1.csv'

def regression(data_input_file):
    data = pd.read_csv(data_input_file)
    print(data.shape)
    model = ols('word_month ~ revert  + reversion_init + word_init + reversion_detal1 + reversion_week1 + word_week1 + reversion_detal2 + reversion_week2 + word_week1 + reversion_detal3 + reversion_week3 + word_week3 + reversion_detal4 + reversion_week4 + word_week4',data).fit()
    print(model.summary())

regression(data_input_file)