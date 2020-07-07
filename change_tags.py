import mysql.connector
import tqdm
mydb = mysql.connector.connect(
	host = 'localhost',
	user = 'root',
	passwd = '1984Luke&'
	)
print(mydb)

path = '/Users/chaofeng/Documents/UZH/2020 Spring/mbm/0501/'


outputfile = path+'change_tags.csv'
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM enwiki.change_tag where ct_tag_id=8  and ct_rev_id is not null;")
myresult = mycursor.fetchall()
lenz = len(myresult)
par = tqdm.tqdm(lenz)
with open(outputfile,'w') as output:
    output.write(";".join(["ct_id", "ct_rc_id","ct_log_id","ct_rev_id","ct_params","ct_tag_id", "cation_name"]))
    for x in myresult:
        par.update(1)
        ct_id = str(x[0])
        ct_rc_id = str(x[1])
        ct_log_id = str(x[2])
        ct_rev_id = str(x[3])
        ct_params = str(x[4])
        ct_tag_id = str(x[5])
        if ct_tag_id ==  8 or ct_tag_id ==  str(8):
            cation_name = 'mw-rollback'

        tag_id = [ct_id, ct_rc_id, ct_log_id, ct_rev_id, ct_params, ct_tag_id, cation_name]
        output.write(";".join(tag_id) + '\n')
print('mapping finished')