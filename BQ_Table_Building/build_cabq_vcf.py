import gcsfs 
from json import loads as json_loads
from json import load as json_load
import yaml
from os.path import expanduser
import io
import pyarrow 
from datetime import datetime 
import csv 
from tqdm import tqdm  as progress 
from google.cloud import bigquery 
from google.cloud import storage 
import pandas as pd 
import json 
import numpy as np 
import re 
import concurrent.futures
import gzip
import sys
import subprocess 
from common_etl.support import confirm_google_vm, merge_csv_files, check_table_existance


def add_labels_and_descriptions(project, 
                                full_table_id,
                                labels_and_desc):
    '''
        @paramaters project, full_table_id

        The function will add in the description, labels, and freindlyName to 
        the published table. 

        @return None 

    '''
    
    client = bigquery.Client(project=project)
    table = client.get_table(full_table_id)
     
    description = ''
    friendly_name = ''
    labels_str = ''
    
    with open(labels_and_desc, 'r') as details_file:
        for line in details_file:
            if line.startswith('description'):
                description += line.split('=')[1]
            elif line.startswith('friendlyName'):
                friendly_name += line.strip().split('=')[1]
            elif line.startswith('labels'):
                labels_str+= line.strip().split('=')[1]
   
    labels = dict(item.strip().split(':') for item in labels_str.strip().split(','))
   
    
    print('Adding Labels, Description, and Friendly name to table')
    table.description = description
    table.friendly_name = friendly_name
    assert table.labels == {}
    table.labels = labels
    assert table.labels == labels
    table = client.update_table(table, ['description','labels','friendlyName']) 

    
def load_to_production_env(publish_project,
                           publish_dataset_id,
                           publish_table_id,
                           schema,
                           scratch_full_table_id,
                           labels_and_desc):
    
    
    client = bigquery.Client(project=publish_project)
    
    full_destination_table_id = f'{publish_project}.{publish_dataset_id}.{publish_table_id}'
    
    check_table_existance(client,
                          full_destination_table_id,
                          schema)
            
    
    add_labels_and_descriptions(publish_project,
                                full_destination_table_id,
                                labels_and_desc)
    
    
    job_config = bigquery.QueryJobConfig(allow_large_results = True,
                                         destination = full_destination_table_id)
    sql = f'''
        SELECT
            *
        FROM 
            `{scratch_full_table_id}`
        '''
    
    # Start the query, passing in the extra configuration 
    table = client.get_table(full_destination_table_id)
    query_job = client.query(sql,
                             job_config = job_config)
    query_job.result()
    print(f'Uploaded records to {table.project}, {table.dataset_id}, {table.table_id}')
    


def load_to_staging_env(dataset_id,
                        table_id,
                        bucket_path,
                        schema_path):
    
    subprocess.call(f"bq load \
                     --replace \
                     --source_format=CSV \
                     --skip_leading_rows=1 \
                     {dataset_id}.{table_id} \
                     {bucket_path} {schema_path}",
                     shell = True )

def push_file_to_bucket(final_merged_csv,
                        bucket_path):
    

    subprocess.call(f"gsutil -o GSUitil:parallel_composit_upload_threshold=150M cp  {final_merged_csv} {bucket_path}", 
                    shell = True)
    


def generate_a_simple_schema(file_path_to_json,
                           dataframe_information_file):
    
    lines_to_skip = ['#',
                    '<class',
                    'RangeIndex',
                    'Data',
                    '-',
                    'dtypes',
                    'memory']
        

    schema = []
    


    with open(dataframe_information_file,'r') as file_in:
        for line in file_in:
            if '#' in line or '<class' in line or 'RangeIndex' in line or 'Data' in line or '-' in line or 'dtypes' in line or 'memory' in line:
                continue

            cleaned_lines = line.strip().split()
            name = cleaned_lines[1]
            dtype = cleaned_lines[2]
            if dtype == 'object':
                json_dict = {'name' : name,
                            'type': 'string'}
                print(f'{name} has been updated to {json_dict["type"]}')
                schema.append(json_dict)
            elif dtype == 'int64':
                json_dict = {'name' : name,
                            'type': 'integer'}
                print(f'{name} has been updated to {json_dict["type"]}')
                schema.append(json_dict)

            elif dtype == 'float64':
                json_dict = {'name' : name,
                            'type': 'float'}
                print(f'{name} has been updated to {json_dict["type"]}')
                schema.append(json_dict)
                
            else:
                json_dict = {'name' : name,
                            'type' : dtype}
                print(f'{name} was not updated {json_dict["type"]}')
                schema.append(json_dict)
    
    with open(file_path_to_json,'w') as out_file:
        out_file.write(json.dumps(schema,
                                indent=2))
    print(f'File has been written to {file_path_to_json}')
    

def write_df_information_to_file(df,
                                 dataframe_information_file):
    
    buffer = io.StringIO()
    
    df.info(verbose = True,
            null_counts = False,
            buf=buffer)
    
    s = buffer.getvalue()
    
    with open(dataframe_information_file, 'w') as f:
        f.write(s)

def simple_schema_builder(program_name,
                          final_merged_csv,
                          dataframe_information_file,
                          home):
    
    if program_name.lower() == 'tcga':
        file_path_to_json = f'{home}/NextGenETL/intermediateFiles/tcga_simple_build_schema.json'
    elif program_name.lower() == 'target':
        file_path_to_json = f'{home}/NextGenETL/intermediateFiles/target_simple_build_schema.json'
    elif program_name.lower() == 'fm':
        file_path_to_json = f'{home}/NextGenETL/intermediateFiles/fm_simple_build_schema.json'
    else:
        print('Program name does not match TCGA, TARGET, or FM')
    
    nrows_to_read = 1000000
    file_path_to_csv = final_merged_csv
    
    df = pd.read_csv(file_path_to_csv,
                     nrows = nrows_to_read,
                     low_memory = False)
    
    write_df_information_to_file(df,
                                 dataframe_information_file)
    
    generate_a_simple_schema(file_path_to_json,
                    dataframe_information_file)
    

    
    

def merge_csv_files(file_1,
                    file_2,
                    file_3):
    
    csv.field_size_limit(10000000)
    csv.field_size_limit()
    with open(file_1,'r') as csv_1, open(file_2,'r') as csv_2, open(file_3,'w') as out_file:
        reader_1 = csv.reader(csv_1)
        reader_2 = csv.reader(csv_2)
        writer = csv.writer(out_file)
        for row_1, row_2 in progress(zip(reader_1,reader_2)):
            writer.writerow(row_1 + row_2)



def create_new_columns(file_1,
                       file_2):
    
    
    '''
        @parameters file1, file2

        This function will take a csv file with the formated gtf file and 
        parse out the 'attribute' column. The parsed information in the attribute 
        column will be transformed into new columns of their own and be written out
        to a csv file. 

        @return None 

    '''
    
    csv.field_size_limit(10000000)
    csv.field_size_limit()
    with open(file_1) as file_in:
        reader = csv.reader(file_in)
        header = next(reader)
        format_column_index = header.index('FORMAT')
        column_names = set()
        for row in progress(reader):
            cell_information = row[format_column_index]
            column_names.update(cell_information.split(':'))
    column_names = list(column_names)
    num_cols = len(column_names)

    with open(file_1) as file_in:
        with open(file_2,'w') as file_out:
            reader = csv.reader(file_in)
            writer = csv.writer(file_out)
            header = next(reader)
            format_column_index = header.index('FORMAT')
            normal_column_index = header.index('NORMAL')
            tumor_column_index = header.index('TUMOR')
            writer.writerow([f'{name}_Normal' for name in column_names]
                            + [f'{name}_Tumor' for name in column_names])
            for row in progress(reader):
                columns = row[format_column_index].split(':')
                tumor_col_values = row[tumor_column_index].split(':')
                if row[normal_column_index] != '':
                    normal_col_values = row[normal_column_index].split(':')
                    column_indicies = [column_names.index(column) for column in columns]
                    row_out = [''] * (num_cols * 2)
                    for column_index, normal_value, tumor_value in zip(column_indicies,normal_col_values,tumor_col_values):
                        row_out[column_index] = normal_value 
                        row_out[column_index + num_cols] = tumor_value
                    writer.writerow(row_out)
                else:
                    column_indicies = [column_names.index(column) for column in columns]
                    row_out = [''] * (num_cols * 2)
                    for column_index, tumor_value in zip(column_indicies,tumor_col_values):
                        row_out[column_index + num_cols] = tumor_value
                    writer.writerow(row_out)


def generate_dataframe(column_headers, 
                       records, 
                       ref_id,
                       file_url, 
                       project_short_name,
                       file_name,
                       analysis_workflow_type,
                       case_barcode,
                       entity_id,file_1, 
                       legacy_tag):
    
    """ 
    @paramters 

    Generates one dataframe from VCF file preserving the format of columns 
    and another dataframe containing meta-header, study type, referenge
    genome and sample ID information.

    This function generates 2 dataframes for each VCF file - one dataframe that corresponds
    to all information in the VCF file with the same format of columns and a second datafrarme 
    that contains meta-header information, reference genome version, study type and sample ID
    information.

    @return Pandas Dataframe vcf_df 


    """

    # Variant Records Dataframe
    vcf_df = pd.DataFrame(records, columns=column_headers)

    legacy_normal_aliquot_barcode = []
    legacy_tumor_aliquot_barcode = []
    for column in vcf_df.columns:
        if '_aliquot' in column or 'TARGET' in column:
            vcf_df.rename(columns={column:'TUMOR'}, inplace=True)
        elif re.search("[DNA]+_.\d\d$",column) or re.search("T\d$",column):
            legacy_tumor_aliquot_barcode.append(column)
            vcf_df.rename(columns={column:'TUMOR'},inplace=True)          
        elif re.search("N\d$",column):
            legacy_normal_aliquot_barcode.append(column)
            vcf_df.rename(columns={column:'NORMAL'},inplace=True)
        else:
            vcf_df.rename(columns={column:column})
    
    if 'NORMAL' not in vcf_df.columns:
        vcf_df['NORMAL'] = np.nan 
    else:
        column_names = [
        'CHROM',
        'POS',
        'ID',
        'REF',
        'ALT',
        'QUAL',
        'FILTER',
        'INFO',
        'FORMAT',
        'TUMOR',
        'NORMAL']     
        vcf_df = vcf_df[column_names]
    
    if legacy_tag == True:
        if len(legacy_tumor_aliquot_barcode) > 0:
            for tumor_id in legacy_tumor_aliquot_barcode:
                vcf_df['legacy_tumor_aliquot_barcode'] = tumor_id
        else:
            vcf_df['legacy_tumor_aliquot_barcode'] = np.nan
        
        if len(legacy_normal_aliquot_barcode) > 0:
            for normal_id in legacy_normal_aliquot_barcode:
                vcf_df['legacy_normal_aliquot_barcode'] = normal_id
        else:
            vcf_df['legacy_normal_aliquot_barcode'] = np.nan

    vcf_df["reference"] = ref_id
    vcf_df["analysis_workflow_type"] = [analysis_workflow_type for wrk_type in range(len(vcf_df))]
    vcf_df["project_short_name"] = [project_short_name for name in range(len(vcf_df))]
    vcf_df["file_gdc_url"] =  [file_url for url in range(len(vcf_df))]
    vcf_df["case_barcode"] = [case_barcode for barcode in range(len(vcf_df))]
    vcf_df["associated_entities__entity_submitter_id"] = [entity_id for sample_id in range(len(vcf_df))]

    return vcf_df  


def parse_vcf(vcf_file):

    """
    @parameters vcf file 
      
    Parses through a VCF file and collects a range of information

    Given each VCF file, this function parses through each row and collects
    meta-header information, Normal and Tumor Sample IDs, reference genome version,
    column headers and records corresponding to each row of information from the file.

    @return meta_data_info[], records[], String ref_id

    """
    
    meta_data_info = []
    records = []
    ref_id = None
    
    for line in vcf_file:
        if line.startswith("##contig"):
            pass
        elif line.startswith("##FORMAT"):
            file_out.write(line.decode())
        elif line.startswith("#"):
            column_headers = line[1:].strip().split()
        else:
            records.append(line.split())

        if line.startswith("##reference"):
            ref_id = line[-17:].strip()
    
    return column_headers, records, ref_id

def parse_zipped_vcf(vcf_file,
                     format_information_file):

    """
    @parameters vcf file 
      
    Parses through a VCF file and collects a range of information

    Given each VCF file, this function parses through each row and collects
    meta-header information, Normal and Tumor Sample IDs, reference genome version,
    column headers and records corresponding to each row of information from the file.

    @return meta_data_info[], records[], String ref_id

    """
    
    meta_data_info = []
    records = []
    ref_id = None
    with open(format_information_file,'a') as file_out:
        for line in vcf_file:
            if line.decode().startswith("##contig"):
                pass
            elif line.decode().startswith("##FORMAT"):
                file_out.write(line.decode())
            elif line.decode().startswith("#"):
                column_headers = line.decode()[1:].strip().split()
            else:
                records.append(line.decode().split())

            if line.decode().startswith("##reference"):
                ref_id = line.decode()[-17:].strip()

    return column_headers, records, ref_id


def start_process(a_file,
                  project_short_name,
                  file_name, 
                  analysis_workflow_type,
                  case_barcode,
                  entity_id,
                  fs,
                  file_1,
                  legacy_tag, 
                  format_information_file,
                  add_header):

    if ".gz" in a_file:
        with fs.open(a_file, 'rb') as binary_file: 
            unzipped_file = gzip.GzipFile(fileobj=binary_file)
            column_headers, records, ref_id = parse_zipped_vcf(unzipped_file.readlines(),
                                                                          format_information_file)
            vcf_df = generate_dataframe(column_headers, 
                                        records, 
                                        ref_id,
                                        a_file,
                                        project_short_name,
                                        file_name,analysis_workflow_type,
                                        case_barcode,
                                        entity_id,file_1, 
                                        legacy_tag)        
    else:       
        with fs.open(a_file, 'r') as vcf_file: 
            meta_data, column_headers, records, ref_id = parse_vcf(vcf_file.readlines(),
                                                                   format_information_file)
            vcf_df = generate_dataframe(column_headers, 
                                        records, 
                                        ref_id,
                                        a_file,
                                        project_short_name,
                                        file_name,
                                        analysis_workflow_type,
                                        case_barcode,
                                        entity_id,
                                        file_1,
                                        legacy_tag)

    with open(file_1, 'a') as out_file:
        vcf_df.to_csv(out_file, header=add_header, index=False)

def query_for_table(filedata_active,
                    gdcid_to_gcsurl,
                    aliquot_to_caseid,
                    program_name):
    """
    @parameters None 

    SQL query to extract certain columns from bigquery using pandas_gbq.

    This SQL query will select the distinct case_barcodes, file_gdc_id, aliquot_barcode,
    file_gdc_url, analysis_workflow_type, and project_short_name from the...
    rel14_aliquot2caseIDmap, rel14_fileData_current and rel14_GDCfileID_to_GCSurl_NEW
    from ISB-CGC's BigQuery Tables found on Google Cloud. 

    @return pandas_dataframe

    """
    client = bigquery.Client(project='isb-cgc-etl')
    
    query_job = (f'''
        SELECT DISTINCT 
            f1.file_gdc_id, 
            f1.analysis_workflow_type, 
            f1.associated_entities__entity_submitter_id, 
            f1.program_name, 
            f1.project_short_name, 
            f1.data_format,
            f1.file_name,
            f2.file_gdc_url,
            f1.case_gdc_id, 
            f3.case_barcode
        FROM 
            `{filedata_active}` as f1,
            `{gdcid_to_gcsurl}` as f2,
            `{aliquot_to_caseid}` as f3
        WHERE 
            f1.case_gdc_id = f3.case_gdc_id
            AND f1.file_gdc_id = f2.file_gdc_id
            AND f1.program_name = "{program_name}"
            AND f1.data_format = "VCF"
            AND f1.analysis_workflow_type NOT LIKE "VCF LiftOver"
    ''')

    df = (
        client.query(query_job).to_dataframe()
    )
    
    file_urls = list(df["file_gdc_url"])
    project_short_names = list(df["project_short_name"])
    file_names = list(df["file_name"])
    analysis_workflow_types = list(df["analysis_workflow_type"])
    case_barcodes = list(df["case_barcode"])
    entity_ids = list(df["associated_entities__entity_submitter_id"])


    return file_urls,project_short_names,file_names,analysis_workflow_types,case_barcodes,entity_ids

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


    program_name = params['PROGRAM_NAME']
    fs = gcsfs.GCSFileSystem(token='google_default')
    legacy_tag = params['LEGACY_TAG']
    max_workers = params['MAX_WORKERS']
    
    # Directory to send each intermediary file to 
    home = expanduser('~')
    variant_call_file_csv = f"{home}/NextGenETL/intermediateFiles/{params['PARSED_VARIANT_CALL_FILE']}"
    format_column_split_csv = f"{home}/NextGenETL/intermediateFiles/{params['FORMAT_COLUMN_SPLIT_FILE']}"
    final_merged_csv = f"{home}/NextGenETL/intermediateFiles/{params['FINAL_MERGED_CSV']}"
    format_information_file = f"{home}/NextGenETL/intermediateFiles/{params['FORMAT_INFO_FILE']}"
    dataframe_information_file = f"{home}/NextGenETL/intermediateFiles/{params['DATAFRAME_INFO_FILE']}"
    
    # Google Cloud Storage bucket path 
    bucket_path = params['BUCKET_PATH']
    
    # Schemas
    schema_path = f"{home}/NextGenETL/intermediateFiles/{program_name.lower()}_simple_build_schema.json"

    # Staging table info for staging env
    staging_project = params['STAGING_PROJECT']
    staging_dataset_id = params['STAGING_DATASET_ID']
    staging_table_id = params['STAGING_TABLE_ID']
    scratch_full_table_id = f'{staging_project}.{staging_dataset_id}.{staging_table_id}'
    
    # Publish table info for production env 
    publish_project = params['PUBLISH_PROJECT']
    publish_dataset_id = params['PUBLISH_DATASET_ID']
    publish_table_id = params['PUBLISH_TABLE_ID']
    schema_with_desc = schema_with_description(params['SCHEMA_WITH_DESCRIPTION'])

    # Path to Labels, Description, and FreindlyName 
    labels_and_desc = params['LABEL_DESCRIPTION_FREINDLYNAME']
    
    

    if params is None:
        print("Bad YAML load")
        return
    
    
    if 'extract_metadata_table' in steps:
        print('* Extracting Meta-Data Table from Google BigQuery!')
        file_urls, project_short_names, file_names, analysis_workflow_types, case_barcodes, entity_ids = query_for_table(params['FILEDATA_ACTIVE'],
                                                                                                                         params['GDCID_TO_GCSURL'],
                                                                                                                         params['ALIQUOT_TO_CASEID'],
                                                                                                                         program_name)

        print(f'Number of files to be processed: {len(file_urls)}')
        print(f'Number of projects in the program, {program_name}: {len(set(project_short_names))}')
        print(f'Number of workflow types for the program, {program_name}: {len(set(analysis_workflow_types))}')
        
        
        pbar = progress(total=len(file_urls))
        file_urls = iter(file_urls)
        project_short_names = iter(project_short_names)
        file_names = iter(file_names)
        analysis_workflow_types = iter(analysis_workflow_types)
        case_barcodes = iter(case_barcodes)
        entity_ids = iter(entity_ids)
        
        
    if 'transform_vcf' in steps:
        print('* Transforming and Parsing the VCF Files!')
        
        # Open an empty csv to store the vcf dataframes (Concatenated VCFs)
        with open(variant_call_file_csv, 'w') as out_file:
            pass
        
        with open(format_information_file, 'w') as format_out:
            pass 
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            add_header = True 
            futures = []
            start_process(
                next(file_urls),
                next(project_short_names),
                next(file_names),
                next(analysis_workflow_types),
                next(case_barcodes),
                next(entity_ids),
                fs,
                variant_call_file_csv,
                legacy_tag,
                format_information_file,
                add_header)
            pbar.update()
        
            running = set()
            for _, a_file, project_short_name, file_name, analysis_workflow_type,case_barcode,entity_id in zip(
                    range(max_workers),
                    file_urls, 
                    project_short_names, 
                    file_names, 
                    analysis_workflow_types,
                    case_barcodes,
                    entity_ids):
                running.add(
                    executor.submit( 
                        start_process, 
                        a_file, 
                        project_short_name, 
                        file_name, 
                        analysis_workflow_type, 
                        case_barcode, 
                        entity_id, 
                        fs, 
                        variant_call_file_csv,
                        legacy_tag, 
                        format_information_file,
                        add_header=False))
            while running:
                done, running = concurrent.futures.wait(running, return_when=concurrent.futures.FIRST_COMPLETED)
                for _ in done:
                    pbar.update()
                for _, a_file, project_short_name, file_name, analysis_workflow_type,case_barcode,entity_id in zip(
                        range(len(done)),
                        file_urls, 
                        project_short_names, 
                        file_names, 
                        analysis_workflow_types,
                        case_barcodes,
                        entity_ids):
                    running.add(
                        executor.submit( 
                            start_process, 
                            a_file, 
                            project_short_name, 
                            file_name, 
                            analysis_workflow_type, 
                            case_barcode, 
                            entity_id, 
                            fs, 
                            variant_call_file_csv,
                            legacy_tag, 
                            format_information_file,
                            add_header=False))
    del(pbar)          
    
    if 'create_new_columns' in steps:
        print('* Creating New Columns!')
        create_new_columns(variant_call_file_csv,
                           format_column_split_csv)
        
    if 'merge_csv_files' in steps:
        print('* Merging CSV Files!')
        merge_csv_files(variant_call_file_csv,
                        format_column_split_csv,
                        final_merged_csv)
        
    if 'build_a_simple_schema' in steps:
        print('* Generating a Simple Schema! ')
        simple_schema_builder(program_name,
                              final_merged_csv,
                              dataframe_information_file,
                              home)
        
    if 'push_csv_to_bucket' in steps:
        print('* Pushing CSV File to Bucket!')
        push_file_to_bucket(final_merged_csv,
                            bucket_path)
        
    if 'load_to_staging_environment' in steps:
        print('* Loading a Table in to a Staging Environment!')
        load_to_staging_env(staging_dataset_id,
                            staging_table_id,
                            bucket_path,
                            schema_path)

    if 'load_to_production_environment' in steps:
        print('* Loading a Table in to a Production Environment!')
        load_to_production_env(publish_project,
                               publish_dataset_id,
                               publish_table_id,
                               schema_with_desc,
                               scratch_full_table_id,
                               labels_and_desc)



if __name__ == '__main__':
    main(sys.argv)