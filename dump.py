import sys
from mwxml import Dump, Page
import fileinput
import bz2file
import gzip
import tqdm
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
#file = 'enwiki-20200501-pages-meta-history1.xml-p1p898.bz2'
file = 'enwiki-20200520-stub-meta-history.xml.gz'
filename = path+file
input = fileinput.FileInput(path+file, openhook=fileinput.hook_compressed)
def xml_to_csv(filename):
    # Construct dump file iterator
    input_file = Dump.from_file(gzip.GzipFile(filename))

    # Open output file
    output_csv = open(filename[0:-2]+"csv",'w')

    # writing header for output csv file
    output_csv.write(";".join(["page_id","page_title","page_ns",
                                "revision_id","revision_parent","timestamp",
                                "contributor_id","contributor_name","comments","model"
                                "bytes"]))
    output_csv.write("\n")

    # Parsing xml and writting proccesed data to output csv
    print("Processing...")

    # Iterate through pages
    par = tqdm.tqdm()
    for page in input_file.pages:
        par.update(1)
        # get page info
        page_id = str(page.id)
        page_title = '|{}|'.format(page.title)
        page_ns = str(page.namespace)
        # Iterate through a page's revisions
        for revision in page:
            if revision != None:
                # get revision info
                revision_id = str(revision.id)
                revision_parent = '-1' if revision.parent_id == None else str(revision.parent_id)
                timestamp = str(revision.timestamp)
                revision_bytes = '-1' if revision.bytes == None else str(revision.bytes)
            else:
                print("A line has imcomplete info about the REVISION metadata "
                        "and therefore it's been removed from the dataset.")
                continue
            
            # get contributor info 
            if revision.user == None:
                print("Revision {} has imcomplete info about the USER metadata "
                        "and therefore it's been removed from the dataset."
                        .format(revision.id))
                continue
            else:
                contributor_id = str(revision.user.id)
                contributor_name = str(revision.user.text)
            
            comment = str(revision.comment)
            model = str(revision.model)


            revision_row = [page_id,page_title,page_ns,
                            revision_id,revision_parent,timestamp,
                            contributor_id,contributor_name, comment, model,
                            revision_bytes]
            #~ print(revision_row)
            output_csv.write(";".join(revision_row) + '\n')

    print("Done processing")
    output_csv.close()
    return True

xml_to_csv(filename)
