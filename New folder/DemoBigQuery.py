from google.cloud import bigquery
import json, os, sys, csv

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "C:\\Users\\meetali\\Desktop\\BlueOcean.json"
client = bigquery.Client()
count=0
query_job = client.query("""
    SELECT
     *
    FROM `blueocean-7a080.com_elcompanies_clinique_blueocean_apac_IOS.app_events_*`
    LIMIT 100 OFFSET 300""")

results = list(query_job.result()) # Waits for job to complete.
# for row in results:
#     print("{} : {} views".format(row.user_id, row.geo_info))
#     print(row)
l=[]
for i in results:
	l.append(i[0])
	# print(l)

	json_data = json.dumps(l)
	# print(json.dumps(l))
	with open('data.json', 'w') as outfile:
		outfile.write(json_data)

#if you are not using utf-8 files, remove the next line

#check if you pass the input file and output file
if sys.argv[1] is not None and sys.argv[2] is not None:
    fileInput = sys.argv[1]
    fileOutput = sys.argv[2]
    inputFile = open(fileInput) #open json file
    outputFile = open(fileOutput, 'w') #load csv file
    data = json.load(inputFile) #load json content
    inputFile.close() #close the input file
    output = csv.writer(outputFile) #create a csv.write
    output.writerow(data[0].keys())  # header row
    dict = {}
    for row in data:
        for k, v in row.items():
            if v not in dict.values():
                dict[k] = v





     print(dict['user_id'])
    for k, v in dict.items():
        print(k, ":", v)
    output.writerow(header)
        # output.writerow(row.values()) #values row