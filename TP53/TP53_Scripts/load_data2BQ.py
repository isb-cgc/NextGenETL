import concurrent.futures
from google.cloud.bigquery import schema
import pandas as pd 
import os 
from google.cloud import bigquery 
import json
import csv 


def upload_to_bq(table_id,
                 csv,
                 schema):
    
    # Job Configuration for BigQuery 
    client = bigquery.Client()
    
    
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV, 
        skip_leading_rows=1,
        write_disposition = "WRITE_TRUNCATE",
    )
    
    # Upload dataframe 2 BigQuery
    with open(csv, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )
    
    job.result()
    table = client.get_table(table_id)
    print(
        f"Load {table.num_rows} rows and {len(table.schema)}, columns to {table_id}"
    )
    
def get_json_schema(basename): 
    with open(os.path.join(f'../P53_Database/P53_data_schema/dbo.{basename}.Table.json')) as json_file:
        data = json.load(json_file)
        
    return data 


def clean_and_write_out(a_file,
                        csv_path_out):
    
    # print('Cleaning ', a_file, '...\n')
    
    file_in = open(a_file)
    read_file = file_in.readlines()    
    with open(csv_path_out, 'w') as file_out:
        reader = csv.reader(read_file, 
                            delimiter=',')
        
        writer = csv.writer(file_out,
                            delimiter=',')
        
        lines = list()
        for row in reader:
            stripped_row = list(map(lambda x: x.strip(), row))
            if stripped_row != []:
                lines.append(stripped_row)

        lines.pop()
        writer.writerows(lines)
    
def process_csv_files(file_name):
    
    # Extract file name for BigQuery TABLE_ID 
    file_name_and_ext = os.path.basename(file_name)
    basename = os.path.splitext(file_name_and_ext)[0]    
    final_csv_path = os.path.join(f'../Cleaned_P53_CSV/{basename}.csv')
    table_id = f'P53_data.{basename}'
    
    schema = get_json_schema(basename)
    
    clean_and_write_out(file_name, 
                        final_csv_path)
    
    upload_to_bq(table_id,
                 final_csv_path,
                 schema)
 

    
def main():
    
    
    arg = input('Would you like to run this in parallel? (Y/n): ')
    
    file_name = ['../P53_Database/P53_data_csv/AA_change.csv',
                '../P53_Database/P53_data_csv/AA_codes.csv', 
                ]
    
    abs_path = [os.path.abspath(a_file) for a_file in file_name]
    
    # Synchronous 
    if arg.lower() == 'n' or arg.lower() == 'no':
        for a_file in abs_path: 
            process_csv_files(a_file)
    
    
    # Parallelized 
    if arg.lower() == 'y' or arg.lower() == 'yes':
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(process_csv_files, abs_path)
    
    
   

if __name__ == '__main__':
    main()