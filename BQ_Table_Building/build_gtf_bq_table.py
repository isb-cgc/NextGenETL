import sys
#from json import loads as json_loads
from json import load as json_load
import yaml
#from google.cloud import storage
from google.cloud import bigquery
from os.path import expanduser
import pandas as pd 
import numpy as np
from alive_progress import alive_bar
import io
import gzip
import csv
#import pyarrow
from google.cloud.exceptions import NotFound
from common_etl.support import confirm_google_vm

# def add_labels_and_descriptions(project, full_table_id):
#     '''
#         @paramaters project, full_table_id
#
#         The function will add in the description, labels, and freindlyName to
#         the published table.
#
#         @return None
#
#     '''
#
#     client = bigquery.Client(project=project)
#     table = client.get_table(full_table_id)
#
#     print('Adding Labels, Description, and Friendly name to table')
#     table.description = 'Data was loaded from the GENCODE reference gene set, release 34, dated Aprilc 2020. These annotations are based on the hg38/GRCh38 reference genome. More details: see Harrow J, et al. (2012) GENCODE: The reference human genome annotation for The ENCODE Project http://www.ncbi.nlm.nih.gov/pubmed/22955987 and ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_human/release_34/gencode.v34.annotation.gtf.gz'
#     table.friendly_name = 'GENCODE V34'
#     assert table.labels == {}
#     labels = {"access": "open",
#               "data_type":"genome_annotation",
#               "source":"gencode",
#               "reference_genome_0":"hg38",
#               "category":"genomic_reference_database",
#               "status":"current"}
#     table.labels = labels
#     table = client.update_table(table, ['description','labels', 'friendlyName'])
#     assert table.labels == labels

def check_table_existance(client, 
                          full_table_id, 
                          schema):


    table_exists = None 

    try:
        client.get_table(full_table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    if table_exists == True:
        print(f'{full_table_id} exists. Making deletion of the table')
        client.delete_table(full_table_id)
        table = bigquery.Table(full_table_id, schema=schema)
        table.clustering_fields = ["CHROM",
                                   "ID",
                                   "analysis_workflow_type",
                                   "project_short_name"]
        table = client.create_table(table)
        print(f"Created clustered table {table.project}, {table.dataset_id}, {table.table_id}")
    else:
        print(f'{full_table_id} does not exist. Creating the table')
        table = bigquery.Table(full_table_id, schema=schema)
        table.clustering_fields = ["CHROM",
                                   "ID",
                                   "analysis_workflow_type",
                                   "project_short_name"]
        table = client.create_table(table)
        print(f"Created clustered table {table.project}, {table.dataset_id}, {table.table_id}")

def publish_table(schema, project, dataset_id, table_id, staging_full_table_id):

    '''
        @parameters schema, project, dataset_id, table_id, staging_full_table_id

        The function will use an SQL query to retrieve the bigquery table from the 
        staging environment and make a push to the production environment including
        the schema description.

        return None

    '''

    client = bigquery.Client(project=project)
    full_table_id = f'{project}.{dataset_id}.{table_id}'

    
    check_table_existance(client,
                          full_table_id,
                          schema)
            
    
    add_labels_and_descriptions(dataset_id,
                                full_table_id)
    

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=full_table_id
    )

    sql = f'''
        SELECT 
            *
        FROM
            {staging_full_table_id}
    '''

    table = client.get_table(full_table_id)
    query_job = client.query(sql, 
                             job_config=job_config)
    query_job.result()
    print(f"Uploaded records to {table.project}, {table.dataset_id}, {table.table_id}")


def upload_to_staging_env(df,project, dataset_id, table_id):

    client = bigquery.Client(project=project)
    

    full_table_id = f'{client.project}.{dataset_id}.{table_id}'
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(
        df, full_table_id, job_config=job_config
    )
    job.result()
    
    table = client.get_table(full_table_id) 
    print(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}')


def split_version_ids(final_merged_csv):

    df = pd.read_csv(final_merged_csv)
    df = df.drop(['attribute'], axis=1)
    
    columns_to_split = ['gene_id',
                        'transcript_id',
                        'exon_id',
                        'ccds_id',
                        'protein_id',
                        'hava_gene',
                        'havana_transcript']

    gene_id_v = []
    transcript_id_v = []
    exon_id_v = []
    ccds_id_v = []
    protein_id_v = []
    havana_gene_v = []
    havana_transcript_v = []

    for v_id in df['gene_id']:
        split_id = v_id.split('.')
        gene_id_v.append(split_id[0])
    
    for v_id in df['transcript_id']:
        if pd.isna(v_id):
            transcript_id_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            transcript_id_v.append(split_id[0])        

    for v_id in df['exon_id']:
        if pd.isna(v_id):
            exon_id_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            exon_id_v.append(split_id[0])
    
    for v_id in df['ccdsid']:
        if pd.isna(v_id):
            ccds_id_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            ccds_id_v.append(split_id[0])
    
    for v_id in df['protein_id']:
        if pd.isna(v_id):
            protein_id_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            protein_id_v.append(split_id[0])

    for v_id in df['havana_gene']:
        if pd.isna(v_id):
            havana_gene_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            havana_gene_v.append(split_id[0])

    for v_id in df['havana_transcript']:
        if pd.isna(v_id):
            havana_transcript_v.append(np.NaN)
        else:
            split_id = v_id.split('.')
            havana_transcript_v.append(split_id[0])
        
    df2 = df.rename(columns={'gene_id': 'gene_id_v',
                             'transcript_id': 'transcript_id_v',
                             'exon_id': 'exon_id_v',
                             'ccdsid': 'ccdsid_v',
                             'protein_id': 'protein_id_v',
                             'havana_gene': 'havana_gene_v',
                             'havana_transcript': 'havana_transcript_v'})

    df2['gene_id'] = gene_id_v
    df2['transcript_id'] = transcript_id_v
    df2['exon_id'] = exon_id_v
    df2['ccdsid'] = ccds_id_v
    df2['protein_id'] = protein_id_v
    df2['havana_gene'] = havana_gene_v
    df2['havana_transcript'] = havana_transcript_v

    return df2

def merge_csv_files(file_1, file_2, file_3, number_of_lines):

    

     with open(file_1,'r') as csv_1, open(file_2,'r') as csv_2, open(file_3,'w') as out_file:
        reader_1 = csv.reader(csv_1)
        reader_2 = csv.reader(csv_2)
        writer = csv.writer(out_file)
        with alive_bar(number_of_lines) as bar:
            for row_1, row_2 in zip(reader_1,reader_2):
                writer.writerow(row_1 + row_2)
                bar()


def create_new_columns(file_1, file_2, number_of_lines):
    '''
        @parameters file1, file2, number_of_lines 

        This function will take a csv file with the formated gtf file and 
        parse out the 'attribute' column. The parsed information in the attribute 
        column will be transformed into new columns of their own and be written out
        to a csv file. 

        @return None 

    '''
    number_of_lines = number_of_lines 
    
    with open(file_1) as in_file:
        reader = csv.reader(in_file)
        header = next(reader)
        attribute_column_index = header.index('attribute')
        column_names = set()

        with alive_bar(number_of_lines) as bar:
            for row in reader:
                cell_information = row[attribute_column_index].split(';')
                for cell_info in cell_information:
                    pair = cell_info.split()
                    if pair != []:
                        header = pair[0]
                        column_names.add(header)
                bar()

        column_names = list(column_names)
        num_cols = len(column_names)

        with open(file_1) as in_file:
            with open(file_2, 'w') as out_file:
                reader = csv.reader(in_file)
                writer = csv.writer(out_file, quoting=csv.QUOTE_NONE, escapechar='', quotechar='')
                header = next(reader)
                attribute_column_index = header.index('attribute')
                writer.writerow(column_names)  
                with alive_bar(number_of_lines) as bar:
                    for row in reader:
                        column_indicies = []
                        row_values_list = []
                        cell_information = row[attribute_column_index].split(';')
                        for cell_info in cell_information:
                                pair = cell_info.split()
                                if pair != []:
                                    header = pair[0]
                                    records = pair[1]
                                    column_indicies.append(column_names.index(header))
                                    row_values_list.append(records)
                        row_out = [''] * num_cols
                        for column_index, row_value, in zip(column_indicies,  row_values_list):
                            row_out[column_index] = row_value
                        writer.writerow(row_out)
                        bar()

def parse_genomic_features_file(a_file, file_1):
    
    column_names = ['seqname', 
                    'source', 
                    'feature',
                    'start', 
                    'end', 
                    'score',
                    'strand', 
                    'frame', 
                    'attribute']

    with open(file_1, 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(column_names)
        with alive_bar(len(a_file)) as bar:
            for line in a_file:
                if line.decode().startswith('#'):
                    pass
                else:
                    line_split = line.decode().strip().split('\t')
                    writer.writerow(line_split)
                bar()


def count_number_of_lines(a_file):

    '''
        @parameters gff_file

        The number lines in the file will counted and returned
        to keep track of the number of lines being generated for 
        CSV file.

        @return int: number_of_lines
    '''

    number_of_lines = 0 
    with gzip.open(a_file, 'rb') as zipped_file:
        for line in zipped_file:
            if line.decode().startswith("##"):
                pass
            else:
                number_of_lines += 1
    print(f'The number of lines in the file: {number_of_lines}')
    
    return number_of_lines

def schema_with_description(path_to_json):

    '''
        @parameters json_file_path 
        
        Give the file path to the json file to retrieve
        the schema for the table which includes the 
        description as well. 

    '''

    with open(path_to_json) as json_file:
        schema = json_load(json_file)

    return schema


def load_config(yaml_config):
    '''
    ----------------------------------------------------------------------------------------------
    The configuration reader. Parses the YAML configuration into dictionaries
    '''

    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None, None 

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']


def main(args):
    '''
    Main Control Flow
    Note that the actual steps run are configured in the YAML input! This allows you to
    e.g. skip previously run steps.
    '''

    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return


    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return
    
    print("job started")

    with open(args[1], mode='r') as yaml_file:
        params, steps, = load_config(yaml_file.read())


    if params is None:
        print("Bad YAML load")
        return
    
    # Put csv files in a select folder 
    home = expanduser('~')
    genomic_feature_file_csv = f"{home}/NextGenETL/intermediateFiles/{params['PARSED_GENOMIC_FORMAT_FILE']}"
    attribute_column_split_csv = f"{home}/NextGenETL/intermediateFiles/{params['ATTRIBUTE_COLUMN_SPLIT_FILE']}"
    final_merged_csv = f"{home}/NextGenETL/intermediateFiles/{params['FINAL_MERGED_CSV']}"


    # Staging table info for staging env
    staging_project = params['STAGING_PROJECT']
    staging_dataset_id = params['STAGING_DATASET_ID']
    staging_table_id = params['STAGING_TABLE_ID']
    scratch_full_table_id = f'{staging_project}.{staging_dataset_id}.{staging_table_id}'
    
    # Publish table info for production env 
    publish_project = params['PUBLISH_PROJECT']
    publish_dataset_id = params['PUBLISH_DATASET_ID']
    publish_table_id = params['PUBLISH_TABLE_ID']
    path_to_json_schema = params['SCHEMA_WITH_DESCRIPTION']

    schema = schema_with_description(path_to_json_schema)

    if 'count_number_of_lines' in steps:
        print('Counting the number of lines in the file')
        number_of_lines = count_number_of_lines(params['FILE_PATH'])

    if 'parse_genomic_features_file' in steps:
        print('Processing Genomic File')
        with gzip.open(params['FILE_PATH'], 'rb') as zipped_file:
            unzipped_file = zipped_file.readlines()
            parse_genomic_features_file(unzipped_file,
                                        genomic_feature_file_csv)

    if 'create_new_columns' in steps:
        print('Creating new columns from the attribute column')
        create_new_columns(genomic_feature_file_csv,
                           attribute_column_split_csv,
                           number_of_lines)
    
    if 'merge_csv_files' in steps:
        print('Merging CSV files')
        merge_csv_files(genomic_feature_file_csv, 
                        attribute_column_split_csv,
                        final_merged_csv,
                        number_of_lines)
    
    if 'split_version_ids' in steps:
        print('Splitting version ids')
        df = split_version_ids(final_merged_csv)
        
    if 'upload_to_staging_env' in steps:
        print('Uploading table to a staging environment')
        upload_to_staging_env(df,
                              staging_project,
                              staging_dataset_id,
                              staging_table_id)
    
    if 'publish_table' in steps:
        print('Publishing table')
        publish_table(schema,
                      publish_project,
                      publish_dataset_id,
                      publish_table_id,
                      scratch_full_table_id)

if __name__ == '__main__':
    main(sys.argv)