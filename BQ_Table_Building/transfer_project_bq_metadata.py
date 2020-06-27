"""

Copyright 2019-2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import sys
import yaml
import io
from git import Repo

from google.cloud import bigquery
from common_etl.support import create_clean_target, generate_dataset_desc_file, create_bq_dataset

'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Drop all BQ datasets in shadow project
'''
def clean_shadow_project(shadow_project):

    client = bigquery.Client(project=shadow_project)

    dataset_ids = []
    for dataset in client.list_datasets():
        dataset_ids.append(dataset.dataset_id)

    for did in dataset_ids:
        client.delete_dataset(did, delete_contents=True, not_found_ok=True)

    return True


'''
----------------------------------------------------------------------------------------------
Copy over the dataset structure:
'''

def shadow_datasets(source_client, shadow_client, source_project, shadow_project):

    dataset_list = source_client.list_datasets()
    for src_dataset in dataset_list:
        src_dataset_obj =  source_client.get_dataset(src_dataset.dataset_id)
        copy_did_suffix = src_dataset.dataset_id.split(".")[-1]
        shadow_dataset_id = "{}.{}".format(shadow_project, copy_did_suffix)

        shadow_dataset = bigquery.Dataset(shadow_dataset_id)

        shadow_dataset.location = src_dataset_obj.location
        shadow_dataset.description = src_dataset_obj.description
        if src_dataset_obj.labels is not None:
            shadow_dataset.labels = src_dataset_obj.labels.copy()

        shadow_client.create_dataset(shadow_dataset)

    return True


'''
----------------------------------------------------------------------------------------------
Create all empty shadow tables
'''

def create_all_shadow_tables(source_client, shadow_client, source_project, src_dataset, src_table, target_project):

    dataset_list = source_client.list_datasets()

    for dataset in dataset_list:
        table_list = list(source_client.list_tables(dataset.dataset_id))
        for tbl in table_list:
            print(str(tbl))
            tbl_obj = source_client.get_table(tbl)

            # Make a completely new copy of the source schema. Do we have to? Probably not. Pananoid.
            targ_schema = []
            for sf in tbl_obj.schema:
                name = sf.name
                field_type = sf.field_type
                mode = sf.mode
                desc = sf.description
                fields = tuple(sf.fields)
                # no "copy constructor"?
                targ_schema.append(bigquery.SchemaField(name, field_type, mode, desc, fields))

            table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
            print(table_id)

            targ_table = bigquery.Table(table_id, schema=targ_schema)
            targ_table.friendlyName = tbl_obj.friendly_name
            targ_table.description = tbl_obj.description
            if tbl_obj.labels is not None:
                targ_table.labels = tbl_obj.labels.copy()
            print(str(targ_table))

            #table = client.create_table(table)  # Make an API request.


            '''
            print("\t- {} table added successfully".format(table_id.split('.')[-1]))
            table = bigquery.Table(table_id, schema=schema_list)
            trg_toks = target_table.split('.')
            trg_proj = trg_toks[0]
            trg_dset = trg_toks[1]
            trg_tab = trg_toks[2]

            src_client = bigquery.Client(src_proj)
            job = src_client.copy_table(source_table, target_table)
            job.result()

            src_table_ref = src_client.dataset(src_dset).table(src_tab)
            s_table = src_client.get_table(src_table_ref)
            src_friendly = s_table.friendly_name

            trg_client = bigquery.Client(trg_proj)
            trg_table_ref = trg_client.dataset(trg_dset).table(trg_tab)
            t_table = src_client.get_table(trg_table_ref)
            t_table.friendly_name = src_friendly




            client.create_table(table)
            '''

    return True

'''
----------------------------------------------------------------------------------------------
Create an empty shadow table
'''

def create_empty_shadow_table(source_client, shadow_client, source_project, src_dataset, src_table, target_project):
    '''
    src_table_ref = source_client.dataset(src_dataset).table(src_table)
    s_table = src_client.get_table(src_table_ref)



    ctrl_client = bigquery.Client(project=source_project)
    dataset_list = ctrl_client.list_datasets()




    for dataset in dataset_list:
        table_list = list(ctrl_client.list_tables(dataset.dataset_id))
        for tbl in table_list:
            tbl_metadata = ctrl_client.get_table(tbl).to_api_repr()
            print(tbl_metadata['schema'])
            print(tbl_metadata['labels'])
            print(tbl_metadata['friendlyName'])
            print(tbl_metadata['description'])
            print(tbl_metadata['numRows'])

        dataset = ctrl_client.get_dataset(dataset.dataset_id)
        print(str(dataset))

        try:
            client = bigquery.Client()
            client.delete_table(table_id, not_found_ok=True)
            table = bigquery.Table(table_id, schema=schema_list)
            client.create_table(table)
            print("\t- {} table added successfully".format(table_id.split('.')[-1]))
        except exceptions.BadRequest as err:
            has_fatal_error("Fatal error for table_id: {}\n{}\n{}".format(table_id, err, schema_list))




        src_toks = source_table.split('.')
        src_proj = src_toks[0]
        src_dset = src_toks[1]
        src_tab = src_toks[2]

        trg_toks = target_table.split('.')
        trg_proj = trg_toks[0]
        trg_dset = trg_toks[1]
        trg_tab = trg_toks[2]

        src_client = bigquery.Client(src_proj)

        src_table_ref = src_client.dataset(src_dset).table(src_tab)
        s_table = src_client.get_table(src_table_ref)
        src_friendly = s_table.friendly_name

        trg_client = bigquery.Client(trg_proj)
        trg_table_ref = trg_client.dataset(trg_dset).table(trg_tab)
        t_table = src_client.get_table(trg_table_ref)
        t_table.friendly_name = src_friendly

        trg_client.update_table(t_table, ['friendlyName'])


        client = bigquery.Client()



        src_table_ref = client.dataset(source_dataset).table(source_table)
        trg_table_ref = client.dataset(target_dataset).table(dest_table)
        src_table = client.get_table(src_table_ref)
        trg_table = client.get_table(trg_table_ref)
        src_schema = src_table.schema
        trg_schema = []
        for src_sf in src_schema:
            trg_sf = bigquery.SchemaField(src_sf.name, src_sf.field_type, description=src_sf.description)
            trg_schema.append(trg_sf)
        trg_table.schema = trg_schema
        client.update_table(trg_table, ["schema"])
        return True


      print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

            for dataset in dataset_list:
                table_list = list(ctrl_client.list_tables(dataset.dataset_id))
                for tbl in table_list:
                    tbl_metadata = ctrl_client.get_table(tbl).to_api_repr()
                    print(tbl_metadata['schema'])
                    print(tbl_metadata['labels'])
                    print(tbl_metadata['friendlyName'])
                    print(tbl_metadata['description'])
                    print(tbl_metadata['numRows'])

                # TODO(developer): Set dataset_id to the ID of the dataset to create.
                # dataset_id = "{}.your_dataset".format(client.project)

                # Construct a full Dataset object to send to the API.
                dataset = bigquery.Dataset(dataset_id)

                # TODO(developer): Specify the geographic location where the dataset should reside.
                dataset.location = "US"

                # Send the dataset to the API for creation, with an explicit timeout.
                # Raises google.api_core.exceptions.Conflict if the Dataset already
                # exists within the project.
                dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
                print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

            dataset = ctrl_client.get_dataset(dataset.dataset_id)
            print(str(dataset))

            try:
                client = bigquery.Client()
                client.delete_table(table_id, not_found_ok=True)
                table = bigquery.Table(table_id, schema=schema_list)
                client.create_table(table)
                print("\t- {} table added successfully".format(table_id.split('.')[-1]))
            except exceptions.BadRequest as err:
                has_fatal_error("Fatal error for table_id: {}\n{}\n{}".format(table_id, err, schema_list))






{'lastModifiedTime': '1592526592933',
'numRows': '3326565',
'schema': {'fields': [{'type': 'STRING', 'name': 'project_short_name', 'mode': 'NULLABLE', 'description': 'Project name abbreviation; the program name appended with a project name abbreviation; eg. TCGA-OV, etc.'}, {'type': 'STRING', 'name': 'case_barcode', 'mode': 'NULLABLE', 'description': 'Original case barcode'}, {'type': 'STRING', 'name': 'sample_barcode', 'mode': 'NULLABLE', 'description': 'sample barcode, eg TCGA-12-1089-01A. One sample may have multiple sets of CN segmentations corresponding to multiple aliquots; use GROUP BY appropriately in queries'}, {'type': 'STRING', 'name': 'aliquot_barcode', 'mode': 'NULLABLE', 'description': 'TCGA aliquot barcode, eg TCGA-12-1089-01A-01D-0517-31'}, {'type': 'STRING', 'name': 'gene_name', 'mode': 'NULLABLE', 'description': 'Gene name e.g. TTN, DDR1, etc.'}, {'type': 'STRING', 'name': 'gene_type', 'mode': 'NULLABLE', 'description': 'The type of genetic element the reads mapped to, eg protein_coding, ribozyme'}, {'type': 'STRING', 'name': 'Ensembl_gene_id', 'mode': 'NULLABLE', 'description': 'The Ensembl gene ID from the underlying file, but stripped of the version suffix -- eg ENSG00000185028'}, {'type': 'STRING', 'name': 'Ensembl_gene_id_v', 'mode': 'NULLABLE', 'description': 'The Ensembl gene ID from the underlying file, including the version suffix  --  eg ENSG00000235943.1'}, {'type': 'INTEGER', 'name': 'HTSeq__Counts', 'mode': 'NULLABLE', 'description': 'Number of mapped reads to each gene as calculated by the Python package HTSeq. https://docs.gdc.cancer.gov/Encyclopedia/pages/HTSeq-Counts/'}, {'type': 'FLOAT', 'name': 'HTSeq__FPKM', 'mode': 'NULLABLE', 'description': 'FPKM is implemented at the GDC on gene-level read counts that are produced by HTSeq1 and generated using custom. scripts https://docs.gdc.cancer.gov/Encyclopedia/pages/HTSeq-FPKM/'}, {'type': 'FLOAT', 'name': 'HTSeq__FPKM_UQ', 'mode': 'NULLABLE', 'description': 'Fragments Per Kilobase of transcript per Million mapped reads (FPKM) is a simple expression level normalization method. The FPKM normalizes read count based on gene length and the total number of mapped reads. https://docs.gdc.cancer.gov/Encyclopedia/pages/HTSeq-FPKM/'}, {'type': 'STRING', 'name': 'case_gdc_id', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for this case (corresponds to the case_barcode).  Can be used to access more information from the GDC data portal like this:   https://portal.gdc.cancer.gov/files/c21b332c-06c6-4403-9032-f91c8f407ba36'}, {'type': 'STRING', 'name': 'sample_gdc_id', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for this sample (corresponds to the sample_barcode), eg a1ec9279-c1a6-4e58-97ed-9ec1f36187c5  --  this can be used to access more information from the GDC data portal'}, {'type': 'STRING', 'name': 'aliquot_gdc_id', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for this aliquot (corresponds to the aliquot_barcode), eg 7fbfdb3e-1fd2-4206-8d2e-7f68e4a15844  --  this can be used to access more information from the GDC data portal'}, {'type': 'STRING', 'name': 'file_gdc_id_counts', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for the file containing counts values(corresponds to the file_barcode)  --  this can be used to access more information from the GDC data portal like this: https://portal.gdc.cancer.gov/files/c21b332c-06c6-4403-9032-f91c8f407ba43'}, {'type': 'STRING', 'name': 'file_gdc_id_fpkm', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for the file containing FPKM values (corresponds to the file_barcode)  --  this can be used to access more information from the GDC data portal like this: https://portal.gdc.cancer.gov/files/c21b332c-06c6-4403-9032-f91c8f407ba43'}, {'type': 'STRING', 'name': 'file_gdc_id_fpkm_uq', 'mode': 'NULLABLE', 'description': 'Unique GDC identifier for the file containing FPKM UQ values(corresponds to the file_barcode)  --  this can be used to access more information from the GDC data portal like this: https://portal.gdc.cancer.gov/files/c21b332c-06c6-4403-9032-f91c8f407ba43'}, {'type': 'STRING', 'name': 'platform', 'mode': 'NULLABLE', 'description': 'Platform used to generate data; either IlluminaHiSeqor IlluminaGA'}]},
'numBytes': '1219199601',
'selfLink': 'https://bigquery.googleapis.com/bigquery/v2/projects/cgc-05-0038/datasets/wjrl_scratch/tables/RNAseq_hg38_r18',
'labels': {'source': 'gdc', 'access': 'open', 'status': 'current', 'category': 'processed_-omics_data', 'data_type': 'gene_expression', 'experimental_strategy': 'rnaseq', 'program': 'organoid', 'reference_genome_0': 'hg38'},
'type': 'TABLE',
'kind': 'bigquery#table',
'friendlyName': 'ORGANOID RNASEQ GENE EXPRESSION',
'description': "Data was extracted from GDC on March 2020. mRNA expression data was generated using Illumina GA or HiSeq sequencing platforms with information from each of the three files (HTSeq Counts, HTSeq FPKM, HTSeq FPKM-UQ) from the GDC's RNAseq pipeline was combine for each aliquot.",
'id': 'cgc-05-0038:wjrl_scratch.RNAseq_hg38_r18',
'creationTime': '1592526592933',
'tableReference': {'datasetId': 'wjrl_scratch', 'tableId': 'RNAseq_hg38_r18', 'projectId': 'cgc-05-0038'},
'etag': 'WEqa2AYAq+VY8laH1GlZYg==',
'numLongTermBytes': '0', }
'location': 'US'




{'lastModifiedTime': '1592526438513', 'numRows': '33', 'schema': {'fields': [{'type': 'STRING', 'name': 'project_short_name', 'mode': 'NULLABLE'}]}, 'etag': 'n0XnF8g4k7c8XeuC8a95TQ==', 'selfLink': 'https://bigquery.googleapis.com/bigquery/v2/projects/cgc-05-0038/datasets/wjrl_scratch/tables/aws_output', 'type': 'TABLE', 'kind': 'bigquery#table', 'id': 'cgc-05-0038:wjrl_scratch.aws_output', 'creationTime': '1592526438513', 'tableReference': {'datasetId': 'wjrl_scratch', 'tableId': 'aws_output', 'projectId': 'cgc-05-0038'}, 'numBytes': '356', 'numLongTermBytes': '0', 'location': 'US'}







            '''

    return True

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input!
This allows you to e.g. skip previously run steps.
'''

def main(args):

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    source_project = params['SOURCE_PROJECT']
    shadow_project = params['SHADOW_PROJECT']

    if 'clean_shadow' in steps:
        success = clean_shadow_project(shadow_project)
        if not success:
            print("clean_target failed")
            return

    if 'shadow_datasets' in steps:
        source_client = bigquery.Client(project=source_project)
        shadow_client = bigquery.Client(project=shadow_project)
        success = shadow_datasets(source_client, shadow_client, source_project, shadow_project)
        if not success:
            print("shadow_datasets failed")
            return

        print('job completed')


    if 'create_all_shadow_tables' in steps:
        source_client = bigquery.Client(project=source_project)
        shadow_client = bigquery.Client(project=shadow_project)
        success = create_all_shadow_tables(source_client, shadow_client, None, None, None, shadow_project)
        if not success:
            print("create_all_shadow_tables failed")
            return

        print('job completed')


if __name__ == "__main__":
    main(sys.argv)