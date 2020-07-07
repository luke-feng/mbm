import sys
from mwxml import Dump, Page
import fileinput
import bz2file
import gzip
import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
file ='enwiki-20200501-pages-logging.xml.gz'

filename = path+file
input = fileinput.FileInput(path+file, openhook=fileinput.hook_compressed)
def xml_to_csv(filename):
    # Construct dump file iterator
    input_file = Dump.from_file(gzip.GzipFile(filename))

    # Open output file
    output_csv = open(filename[0:-2]+"1.csv",'w')

    # writing header for output csv file
    output_csv.write(";".join(["logitem_id","page_namespace","logtitle","timestamp","year","date",
                                "contributor_id","contributor_name",
                                "type","action","comment"]))
    output_csv.write("\n")

    # Parsing xml and writting proccesed data to output csv
    print("Processing...")
    par = tqdm.tqdm()
    # Iterate through logitems
    for item in input_file.log_items:
        par.update(1)
        timestamp = str(item.timestamp)
        year = timestamp[:4]
        logitem_id = str(item.id)
        date = timestamp[0:10]
        types = str(item.type)
        action = str(item.action)
        comment = str(item.comment)
        if item.user == None:
            contributor_id = 'none'
            contributor_name = 'none'
            continue
        else:
            contributor_id = str(item.user.id)
            contributor_name = str(item.user.text)

        if item.page == None:
            page_namespace = 'none'
            logtitle = 'none'
            continue
        else:
            page_namespace = str(item.page.namespace)
            logtitle = str(item.page.title)

        revision_row = [logitem_id,page_namespace,logtitle,timestamp,year, date,
                            contributor_id,contributor_name,
                            types,action,
                            comment]
        #~ print(revision_row)
        output_csv.write(";".join(revision_row) + '\n')

    print("Done processing")
    output_csv.close()
    return True

xml_to_csv(filename)
