#!/usr/bin/python
import csv, re, os, sys, getopt
from datetime import datetime

# from google.cloud import bigquery

input_file_name = "source.csv"
output_file_name = "staging.csv"
batch_num_col_name = "BATCH_NUM"

def main(argv):
    global input_file_name
    global output_file_name
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print ('test.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file_name = arg
        elif opt in ("-o", "--ofile"):
            output_file_name = arg
    print('Input file is', input_file_name)
    print('Output file is', output_file_name)

if __name__ == "__main__":
    main(sys.argv[1:])
    print("Program start")
    # Checking output file exists
    if os.path.exists(output_file_name):
        os.remove(output_file_name)
    else:
        print("The output file does not exist")

    # Generate batch num
    batch_num = datetime.now().strftime('%Y%m%d%H%M%S')
    print("Batch Num:", batch_num)

    new_rows = []

    with open(input_file_name, newline='') as csvfile:

        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames # Reading headers from CSV

        # Convert to table names
        table_names = []
        for header in headers:
            table_name = header.strip().upper()
            table_name = re.sub(r'[^A-Za-z0-9 ]+', '', table_name).replace(' ', '_')
            # print("  Table name:", table_name)
            table_names.append(table_name)
        table_names.append(batch_num_col_name) # Adding a batch number column at the end. 

        
        for row in reader:
            new_row = []
            for item in row:
                new_row.append(row[item])
            new_row.append(batch_num)
            new_rows.append(new_row)
            
    # Preparing the output file
    with open(output_file_name, 'w', newline='') as output_file:
        # Preparing output file headers
        writer = csv.DictWriter(output_file, fieldnames=table_names)
        writer.writeheader()
        for new_row in new_rows:
            writer.writerow(dict(zip(table_names, new_row)))

        # line_num = 1
        # for row in reader:  
        #     print("Processing Row Num:", line_num)
        #     print(row)
        #     line_num = line_num + 1


    ###
    # 
    # Comment for Google part. 
    # 
    ###
    
    # # Construct a BigQuery client object.
    # client = bigquery.Client()
    # query = """
    #     SELECT name, SUM(number) as total_people
    #     FROM `bigquery-public-data.usa_names.usa_1910_2013`
    #     WHERE state = 'TX'
    #     GROUP BY name, state
    #     ORDER BY total_people DESC
    #     LIMIT 20
    # """
    # query_job = client.query(query)  # Make an API request.

    # print("The query data:")
    # for row in query_job:
    #     # Row values can be accessed by field name or index.
    #     print("name={}, count={}".format(row[0], row["total_people"]))