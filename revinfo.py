import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
change_tag = path + 'change_tags.csv'
rev_file = path + 'meta/'+'2001_meta.csv'

outputfile = path + 'rev_info' + 'rev_revert2001.csv'


def get_rev_ct_mapping(change_tag):
    rev_ct = {}
    par = tqdm.tqdm()
    with open(change_tag,'r') as ct:
        for line in ct:
            par.update(1)
            line = line.rstrip()
            tokens = line.split(";")
            rev_id = tokens[3]
            rev_ct[rev_id] = line
    return rev_ct

rev_ct = get_rev_ct_mapping(change_tag)
for i in range(2017,2018):
    inputfile = path + 'meta/'+str(i)+'_meta.csv'
    with open(inputfile,'r') as rev:
        outputfile = outputfile = path + 'rev_info/' + str(i)+'.csv'
        with open(outputfile, 'w') as output:
            output.write((";".join(["page_id","page_title","page_ns",
                                        "revision_id","revision_parent","timestamp",
                                        "contributor_id","contributor_name","comments","model",
                                        "bytes","year","date","ct_id", "ct_rc_id","ct_log_id",
                                        "ct_rev_id","ct_params","ct_tag_id", "cation_name"])))
            output.write("\n")
            par = tqdm.tqdm()
            for line in rev:
                par.update(1)
                line = line.rstrip()
                tokens = line.split(";")
                rev_id = tokens[3]
                if rev_id in rev_ct:
                    n_line = line + ';'+rev_ct[rev_id]
                    output.write(n_line + '\n')
