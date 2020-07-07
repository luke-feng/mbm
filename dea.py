import bz2
import fileinput
from lxml import etree as et
path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'
#file = 'enwiki-20200501-pages-meta-history1.xml-p1p898.bz2'
file ='enwiki-20200520-stub-meta-history.xml.gz'
file = '/Volumes/DC/wikidata/20100312/enwiki-20100312-pages-meta-history.xml.bz2'
input = fileinput.FileInput(file, openhook=fileinput.hook_compressed)
i=0
act = {}
for line in input:
    if i < 200:
        '''if '<action>restore</action>' in line.decode("utf-8") :
            act[line.decode("utf-8")] = '''''
        print(i,line.decode("utf-8"))
    else:
        break
    i+=1
for item in act:
    print(item)
