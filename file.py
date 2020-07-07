import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
input_file = 'enwiki-20200520-stub-meta-history.xml.csv'
out_files = []
years = range(2000,2021)
par = tqdm.tqdm()
with open(path + input_file,'r') as input:
    for file in range(2000,2021):
        file_name = path+'meta/'+str(file)+'_meta.csv'
        output_csv = open(file_name,'w')
        output_csv.write(";".join(["page_id","page_title","page_ns",
                                "revision_id","revision_parent","timestamp",
                                "contributor_id","contributor_name","comments","model",
                                "bytes","year","date"]))
        output_csv.write("\n")
        out_files.append(output_csv)
    
    for line in input:
        par.update(1)
        line = line.rstrip()
        tokens = line.split(';')
        if len(tokens) >= 6:
            y = tokens[5][0:4]
            d = tokens[5][0:10]
            for year in years:
                if y == str(year):
                    i = year - 2000
                    ny = ";".join([line,y,d])
                    out_files[i].write(ny + '\n')

        

