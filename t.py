import sys
from mwxml import Dump, Page
import fileinput
import bz2file
import gzip
import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/meta-history/'

file = 'enwiki-20200601-pages-meta-history1.xml-p1p895.bz2'
filename = path+file

def xml_to_csv(filename):
    # Construct dump file iterator
    input_file = Dump.from_file(bz2file.BZ2File(filename))

    print("Processing...")
    # Open output file
    output_csv = open(filename[0:-3]+"2csv",'w')

    # writing header for output csv file
    output_csv.write(";".join(["page_id","page_title","page_ns",
                                "revision_id","revision_parent","timestamp",
                                "contributor_id","contributor_name","comments","model"
                                "bytes"]))
    output_csv.write("\n")
    # Iterate through pages
    par = tqdm.tqdm()
    for page in input_file.pages:
        par.update(1)
        # get page info
        page_id = str(page.id)
        page_title = '|{}|'.format(page.title)
        page_ns = str(page.namespace)
        if page_id == '12':
            for revision in page:
                if revision != None:
                    # get revision info
                    revision_id = str(revision.id)
                    if revision_id == '876580929':
                        text = str(revision.text)
                        revision_parent = '-1' if revision.parent_id == None else str(revision.parent_id)
                        timestamp = str(revision.timestamp)
                        revision_bytes = '-1' if revision.bytes == None else str(revision.bytes)
                    
                
                        contributor_id = str(revision.user.id)
                        contributor_name = str(revision.user.text)
                
                        comment = str(revision.comment)
                        model = str(revision.model)


                        revision_row = [page_id,page_title,page_ns,
                                revision_id,revision_parent,timestamp,
                                contributor_id,contributor_name, comment, model,
                                revision_bytes,text]
                        #~ print(revision_row)
                        output_csv.write(";".join(revision_row) + '\n')
                        return

    print("Done processing")
    output_csv.close()
    return True

xml_to_csv(filename)
