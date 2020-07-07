import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/rev_info/'
rev_info_file1 = path + '2018.csv'
rev_info_file2 = path + '2019.csv'
rev_info_file3 = path + '2017.csv'
outputfile = path + 'page_id.csv'
page = {}

with open(rev_info_file1, 'r') as rf1, open(rev_info_file2, 'r') as rf2, open(rev_info_file3, 'r') as rf3, open(outputfile, 'w') as output:
    par = tqdm.tqdm()
    for line in rf1:
        par.update(1)
        tokens = line.split(';')
        page_id = tokens[0]
        page[page_id] = ''
    par = tqdm.tqdm()
    for line in rf2:
        par.update(1)
        tokens = line.split(';')
        page_id = tokens[0]
        page[page_id] = ''
    par = tqdm.tqdm()
    for line in rf3:
        par.update(1)
        tokens = line.split(';')
        page_id = tokens[0]
        page[page_id] = ''
    par = tqdm.tqdm()
    for p in page:
        par.update(1)
        output.write(p+'\n')