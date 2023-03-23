"""
Copyright 2019, Institute for Systems Biology
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

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
VM NEEDS 26 GB MEMORY FOR THIS TO NOT DIE IN MERGE MODE, MORE IF DEBUG == True
In merge mode, the job needed 18 G peak to run, hitting the highest levels on UCEC
and UCS (which are processed near the very end). This required running on a n1-highmem-4
(4 vCPUs, 26 GB memory) machine, and took about 40 minutes. When the DEBUG Logging was
enabled, performance was degraded, and I moved to using a n1-highmem-8 (8 vCPUs, 52 GB memory)
machine. Note that only one processor gets used, so that is just increasing the memory.
That took 53 minutes. It appears that DEBUG logging created 37.5 million lines for
reading, 187 million lines for writing.
'''

import sys
import os
import yaml
import shutil
import zipfile
import io
import re
from json import loads as json_loads
from os.path import expanduser
from datetime import date
from types import SimpleNamespace
import gzip

from common_etl.support import create_clean_target, pull_from_buckets, build_file_list, generic_bq_harness, \
    upload_to_bucket, csv_to_bq, delete_table_bq_job, \
    build_pull_list_with_bq, write_table_schema_with_generic, update_dir_from_git, \
    create_schema_hold_list, get_the_bq_manifest, confirm_google_vm, \
    update_schema_tags, publish_tables_and_update_schema, bq_table_exists

from common_etl.utils import find_types

def load_config(yaml_config):
    """
    The configuration reader. Parses the YAML configuration into dictionaries
    :param yaml_config: file location of the YAML configuration
    :type yaml_config: basestring
    :return: dictionary of configurations
    :rtype: dict
    """
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch, case_table):
    """
    Gather and add sample information to the draft table from the aliquot table
    :param temp_table: draft table to add the aliquot information to
    :type temp_table: basestring
    :param aliquot_table: metadata table id for the aliquot information
    :type aliquot_table: basestring
    :param target_dataset: dataset id for the new table
    :type target_dataset: basestring
    :param dest_table: table id for the new table
    :type dest_table: basestring
    :param do_batch: If the BQ job should be run in batch mode
    :type do_batch: bool
    :param case_table: metadata table id for the case information
    :type case_table: basestring
    :return: if the SQL query worked
    :rtype: bool
    """
    sql = attach_barcodes_sql(temp_table, aliquot_table, case_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

def attach_barcodes_sql(maf_table, aliquot_table, case_table):
    """
    SQL for the attach_barcodes function
    :param maf_table: table id for the draft maf table
    :type maf_table: basestring
    :param aliquot_table: table id for the aliquot table
    :type aliquot_table: basestring
    :param case_table: table id for the case table
    :type case_table: basestring
    :return: Formatted string SQL query
    :rtype: basestring
    """
    return f"""
        WITH
        a1 AS (SELECT b.project_id AS project_short_name,
                      a.case_id AS case_gdc_id,
                      b.aliquot_barcode AS aliquot_barcode_tumor,
                      b.sample_barcode AS sample_barcode_tumor,
                      a.Tumor_Aliquot_UUID AS aliquot_gdc_id_tumor,
                      a.Matched_Norm_Aliquot_UUID AS aliquot_gdc_id_normal,
                      a.Start_Position,
          a.Chromosome
            FROM
              `{maf_table}` AS a JOIN `{aliquot_table}` AS b ON a.Tumor_Aliquot_UUID = b.aliquot_gdc_id),
        a2 AS (SELECT a1.project_short_name,
                      c.case_barcode,
                      a1.sample_barcode_tumor,
                      c.sample_barcode AS sample_barcode_normal,
                      a1.aliquot_barcode_tumor,
                      c.aliquot_barcode AS aliquot_barcode_normal,
                      a1.aliquot_gdc_id_tumor,
                      a1.Start_Position, 
          a1.Chromosome
            FROM a1 JOIN `{aliquot_table}` AS c ON a1.aliquot_gdc_id_normal = c.aliquot_gdc_id
            WHERE c.case_gdc_id = a1.case_gdc_id)
        SELECT a2.project_short_name,
               a2.case_barcode,
               d.primary_site,
               a2.sample_barcode_tumor,
               a2.sample_barcode_normal,
               a2.aliquot_barcode_tumor,
               a2.aliquot_barcode_normal,
               a2.aliquot_gdc_id_tumor,
               a2.Start_Position, 
       a2.Chromosome
        FROM a2 JOIN `{case_table}` AS d ON a2.case_barcode = d.case_barcode
    """

def barcode_raw_table_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch):
    """
    Glue the New Info to the Raw Data Table
    :param maf_table: table id for the draft maf table
    :type maf_table: basestring
    :param barcode_table: table id for the barcode table
    :type barcode_table: basestring
    :param target_dataset: dataset id for the new table
    :type target_dataset: basestring
    :param dest_table: table id for the new table
    :type dest_table: basestring
    :param do_batch: If the BQ job should be run in batch mode
    :type do_batch: bool
    :return: if the SQL query worked
    :rtype: bool
    """
    sql = final_join_sql(maf_table, barcode_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

def final_join_sql(maf_table, barcode_table):
    """
    SQL string for the barcode_raw_table_merge
    :param maf_table: table id for the draft maf table
    :type maf_table: basestring
    :param barcode_table: table id for the barcode table
    :type barcode_table: basestring
    :return: Formatted string SQL query
    :rtype: basestring
    """
    return f"""
         SELECT a.project_short_name,
                a.case_barcode,
                a.primary_site,
                b.*,
                a.sample_barcode_tumor,
                a.sample_barcode_normal,
                a.aliquot_barcode_tumor, 
                a.aliquot_barcode_normal,
         FROM `{barcode_table}` as a JOIN `{maf_table}` as b 
         ON a.aliquot_gdc_id_tumor = b.Tumor_Aliquot_UUID AND a.Start_Position = b.Start_Position AND a.Chromosome = b.Chromosome
    """

def file_info(filepath):
    """
    file_info() function Author: Sheila Reynolds
    File name includes important information, e.g. the program name and the caller. Extract that
    out along with name and ID.
    :param filepath:
    :type filepath:
    :return: the file UUID
    :rtype: basestring
    """
    norm_path = os.path.normpath(filepath)
    path_pieces = norm_path.split(os.sep)
    file_uuid = path_pieces[-2]

    return file_uuid

def clean_header_names(header_list, fields_to_fix):
    """
    Change header names based on the list in yaml configuration file
    :param header_list: List of headers
    :type header_list: list
    :param fields_to_fix: List of dictionaries of fields to fix
    :type fields_to_fix: list
    :return: Updated header list
    :rtype: list
    """
    for header_name in range(len(header_list)):
        for dict in fields_to_fix:
            original, new = next(iter(dict.items()))

            if header_list[header_name] == original:
                header_list[header_name] = new

    return header_list

def process_callers(callers_str, callers):
    """
    Separate the Callers into their own columns
    The maf files has one column with a semicolon delimited with the callers in it.
    :param callers_str: string of callers from raw file
    :type callers_str: basestring
    :param callers: list of callers from yaml file
    :type callers: list
    :return: list of callers in the raw file
    :rtype: list
    """
    line_callers = callers_str.rstrip('\n').split(';')
    caller_list = dict.fromkeys(callers, 'No')
    for caller in line_callers:
        is_star = re.search(r'\*', caller)
        if caller.rstrip('*') in caller_list.keys():
            if is_star:
                caller_list[caller.rstrip('*')] = 'Yes*'
            else:
                caller_list[caller] = 'Yes'
    return caller_list

def concat_all_files(all_files, one_big_tsv, callers, fields_to_fix):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    :param all_files: list of file paths
    :type all_files: list
    :param one_big_tsv: name of file to create
    :type one_big_tsv: basestring
    :param callers: list of callers
    :type callers: list
    :param fields_to_fix: list of fixed fields
    :type fields_to_fix: list
    """
    print(f"building {one_big_tsv}")
    first = True
    header_id = None
    caller_field_index = 0
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            toss_zip = False
            if filename.endswith('.zip'):
                dir_name = os.path.dirname(filename)
                print(f"Unzipping {filename}")
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall(dir_name)
                use_file_name = filename[:-4]
                toss_zip = True
            elif filename.endswith('.gz'):
                use_file_name = filename[:-3]
                print(f"Uncompressing {filename}")
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename
            with open(use_file_name, 'r') as readfile:
                file_uuid = file_info(use_file_name)
                for line in readfile:
                    # Bypass comments in MAF file
                    if not line.startswith('#'):
                        if first:
                            header_list = line.rstrip('\n').split('\t')
                            header_id = header_list[0]
                            header_names = clean_header_names(header_list, fields_to_fix)
                            caller_field_index = header_names.index('callers')
                            header_line = '\t'.join(header_names)
                            outfile.write(header_line)
                            outfile.write('\t')
                            outfile.write('file_gdc_id')
                            for field in callers:
                                outfile.write('\t')
                                outfile.write(field)
                            outfile.write('\n')
                            first = False
                        if not line.startswith(header_id):
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write(file_uuid)
                            caller_data = process_callers(line.rstrip('\n').split('\t')[caller_field_index], callers)
                            for caller in callers:
                                outfile.write('\t')
                                outfile.write(caller_data[caller])
                            outfile.write('\n')
                if toss_zip:
                    os.remove(use_file_name)


def merge_samples_by_aliquot(input_table, output_table, target_dataset, callers, do_batch):
    """
    Some samples are pooled and their lines need to be merged by aliquot
    :param input_table: table to merge samples
    :type input_table: basestring
    :param output_table: name of new table
    :type output_table: basestring
    :param target_dataset: dataset for the new table
    :type target_dataset: basestring
    :param callers: list of callers
    :type callers: list
    :param do_batch: If the BQ job should be run in batch mode
    :type do_batch: bool
    :return: if the SQL query worked
    :rtype: bool
    """
    sql = merge_samples_by_aliquot_sql(input_table, callers)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')


def merge_samples_by_aliquot_sql(input_table, callers):
    """
    SQL for merge_samples_by_aliquot
    :param input_table: table to merge samples
    :type input_table: basestring
    :param callers: list of callers
    :type callers: list
    :return: formatted SQL string
    :rtype: basestring
    """
    joined_callers = ", ".join(callers)

    return f"""
    SELECT 
        project_short_name,
        case_barcode,
        primary_site,
        Hugo_Symbol,
        Entrez_Gene_Id,
        Center,
        NCBI_Build,
        Chromosome,
        Start_Position,
        End_Position,
        Strand,
        Variant_Classification,
        Variant_Type,
        Reference_Allele,
        Tumor_Seq_Allele1,
        Tumor_Seq_Allele2,
        dbSNP_RS,
        dbSNP_Val_Status,
        Tumor_Aliquot_Barcode,
        Matched_Norm_Aliquot_Barcode,
        Match_Norm_Seq_Allele1,
        Match_Norm_Seq_Allele2,
        Tumor_Validation_Allele1,
        Tumor_Validation_Allele2,
        Match_Norm_Validation_Allele1,
        Match_Norm_Validation_Allele2,
        Verification_Status,
        Validation_Status,
        Mutation_Status,
        Sequencing_Phase,
        Sequence_Source,
        Validation_Method,
        Score,
        BAM_File,
        Sequencer,
        Tumor_Aliquot_UUID,
        Matched_Norm_Aliquot_UUID,
        HGVSc,
        HGVSp,
        HGVSp_Short,
        Transcript_ID,
        Exon_Number,
        t_depth,
        t_ref_count,
        t_alt_count,
        n_depth,
        n_ref_count,
        n_alt_count,
        all_effects,
        Allele,
        Gene,
        Feature,
        Feature_type,
        One_Consequence,
        Consequence,
        cDNA_position,
        CDS_position,
        Protein_position,
        Amino_acids,
        Codons,
        Existing_variation,
        DISTANCE,
        TRANSCRIPT_STRAND,
        SYMBOL,
        SYMBOL_SOURCE,
        HGNC_ID,
        BIOTYPE,
        CANONICAL,
        CCDS,
        ENSP,
        SWISSPROT,
        TREMBL,
        UNIPARC,
        UNIPROT_ISOFORM,
        RefSeq,
        MANE,
        APPRIS,
        FLAGS,
        SIFT,
        PolyPhen,
        EXON,
        INTRON,
        DOMAINS,
        ThousG_AF,
        ThousG_AFR_AF,
        ThousG_AMR_AF,
        ThousG_EAS_AF,
        ThousG_EUR_AF,
        ThousG_SAS_AF,
        ESP_AA_AF,
        ESP_EA_AF,
        gnomAD_AF,
        gnomAD_AFR_AF,
        gnomAD_AMR_AF,
        gnomAD_ASJ_AF,
        gnomAD_EAS_AF,
        gnomAD_FIN_AF,
        gnomAD_NFE_AF,
        gnomAD_OTH_AF,
        gnomAD_SAS_AF,
        MAX_AF,
        MAX_AF_POPS,
        gnomAD_non_cancer_AF,
        gnomAD_non_cancer_AFR_AF,
        gnomAD_non_cancer_AMI_AF,
        gnomAD_non_cancer_AMR_AF,
        gnomAD_non_cancer_ASJ_AF,
        gnomAD_non_cancer_EAS_AF,
        gnomAD_non_cancer_FIN_AF,
        gnomAD_non_cancer_MID_AF,
        gnomAD_non_cancer_NFE_AF,
        gnomAD_non_cancer_OTH_AF,
        gnomAD_non_cancer_SAS_AF,
        gnomAD_non_cancer_MAX_AF_adj,
        gnomAD_non_cancer_MAX_AF_POPS_adj,
        CLIN_SIG,
        SOMATIC,
        PUBMED,
        TRANSCRIPTION_FACTORS,
        MOTIF_NAME,
        MOTIF_POS,
        HIGH_INF_POS,
        MOTIF_SCORE_CHANGE,
        miRNA,
        IMPACT,
        PICK,
        VARIANT_CLASS,
        TSL,
        HGVS_OFFSET,
        PHENO,
        GENE_PHENO,
        CONTEXT,
        tumor_submitter_uuid,
        normal_submitter_uuid,
        case_id,
        GDC_FILTER,
        COSMIC,
        hotspot,
        RNA_Support,
        RNA_depth,
        RNA_ref_count,
        RNA_alt_count,
        callers,
        file_gdc_id,
        {joined_callers},
        string_agg(distinct sample_barcode_tumor, ';') as sample_barcode_tumor, 
        string_agg(distinct sample_barcode_normal, ';') as sample_barcode_normal, 
        aliquot_barcode_tumor, 
        aliquot_barcode_normal
    FROM 
        `{input_table}`
    group by 
        project_short_name,
        case_barcode,
        primary_site,
        Hugo_Symbol,
        Entrez_Gene_Id,
        Center,
        NCBI_Build,
        Chromosome,
        Start_Position,
        End_Position,
        Strand,
        Variant_Classification,
        Variant_Type,
        Reference_Allele,
        Tumor_Seq_Allele1,
        Tumor_Seq_Allele2,
        dbSNP_RS,
        dbSNP_Val_Status,
        Tumor_Aliquot_Barcode,
        Matched_Norm_Aliquot_Barcode,
        Match_Norm_Seq_Allele1,
        Match_Norm_Seq_Allele2,
        Tumor_Validation_Allele1,
        Tumor_Validation_Allele2,
        Match_Norm_Validation_Allele1,
        Match_Norm_Validation_Allele2,
        Verification_Status,
        Validation_Status,
        Mutation_Status,
        Sequencing_Phase,
        Sequence_Source,
        Validation_Method,
        Score,
        BAM_File,
        Sequencer,
        Tumor_Aliquot_UUID,
        Matched_Norm_Aliquot_UUID,
        HGVSc,
        HGVSp,
        HGVSp_Short,
        Transcript_ID,
        Exon_Number,
        t_depth,
        t_ref_count,
        t_alt_count,
        n_depth,
        n_ref_count,
        n_alt_count,
        all_effects,
        Allele,
        Gene,
        Feature,
        Feature_type,
        One_Consequence,
        Consequence,
        cDNA_position,
        CDS_position,
        Protein_position,
        Amino_acids,
        Codons,
        Existing_variation,
        DISTANCE,
        TRANSCRIPT_STRAND,
        SYMBOL,
        SYMBOL_SOURCE,
        HGNC_ID,
        BIOTYPE,
        CANONICAL,
        CCDS,
        ENSP,
        SWISSPROT,
        TREMBL,
        UNIPARC,
        UNIPROT_ISOFORM,
        RefSeq,
        MANE,
        APPRIS,
        FLAGS,
        SIFT,
        PolyPhen,
        EXON,
        INTRON,
        DOMAINS,
        ThousG_AF,
        ThousG_AFR_AF,
        ThousG_AMR_AF,
        ThousG_EAS_AF,
        ThousG_EUR_AF,
        ThousG_SAS_AF,
        ESP_AA_AF,
        ESP_EA_AF,
        gnomAD_AF,
        gnomAD_AFR_AF,
        gnomAD_AMR_AF,
        gnomAD_ASJ_AF,
        gnomAD_EAS_AF,
        gnomAD_FIN_AF,
        gnomAD_NFE_AF,
        gnomAD_OTH_AF,
        gnomAD_SAS_AF,
        MAX_AF,
        MAX_AF_POPS,
        gnomAD_non_cancer_AF,
        gnomAD_non_cancer_AFR_AF,
        gnomAD_non_cancer_AMI_AF,
        gnomAD_non_cancer_AMR_AF,
        gnomAD_non_cancer_ASJ_AF,
        gnomAD_non_cancer_EAS_AF,
        gnomAD_non_cancer_FIN_AF,
        gnomAD_non_cancer_MID_AF,
        gnomAD_non_cancer_NFE_AF,
        gnomAD_non_cancer_OTH_AF,
        gnomAD_non_cancer_SAS_AF,
        gnomAD_non_cancer_MAX_AF_adj,
        gnomAD_non_cancer_MAX_AF_POPS_adj,
        CLIN_SIG,
        SOMATIC,
        PUBMED,
        TRANSCRIPTION_FACTORS,
        MOTIF_NAME,
        MOTIF_POS,
        HIGH_INF_POS,
        MOTIF_SCORE_CHANGE,
        miRNA,
        IMPACT,
        PICK,
        VARIANT_CLASS,
        TSL,
        HGVS_OFFSET,
        PHENO,
        GENE_PHENO,
        CONTEXT,
        tumor_submitter_uuid,
        normal_submitter_uuid,
        case_id,
        GDC_FILTER,
        COSMIC,
        hotspot,
        RNA_Support,
        RNA_depth,
        RNA_ref_count,
        RNA_alt_count,
        callers,
        file_gdc_id,
        {joined_callers},
        aliquot_barcode_tumor, 
        aliquot_barcode_normal"""


def create_per_program_table(input_table, output_table, program, target_dataset, do_batch):
    """
    Split the combined table into tables per program
    :param input_table: Combined draft table
    :type input_table: basestring
    :param output_table: name for the new table
    :type output_table: basestring
    :param program: Program to filter the table on
    :type program: basestring
    :param target_dataset: dataset to store the new table
    :type target_dataset: basestring
    :param do_batch: If the BQ job should be run in batch mode
    :type do_batch: bool
    :return: if the SQL query worked
    :rtype: bool
    """
    sql = sql_create_per_program_table(input_table, program)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')


def sql_create_per_program_table(input_table, program):
    """
    SQL for create_per_program_table
    :param input_table: Combined draft table
    :type input_table: basestring
    :param program: Program to filter the table on
    :type program: basestring
    :return: formatted SQL query
    :rtype: basestring
    """
    return f"""
        SELECT *
        FROM `{input_table}`
        WHERE project_short_name LIKE '{program}%'
    """


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
'''


def main(args):
    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return

    if len(args) != 2:
        print(" ")
        print(f" Usage : {args[0]} <configuration_yaml>")
        return

    print('job started')

    # Get the YAML config loaded:
    with open(args[1], mode='r') as yaml_file:
        params_dict, steps = load_config(yaml_file.read())
        params = SimpleNamespace(**params_dict)

    if params_dict is None:
        print("Bad YAML load")
        return

    # Which table are we building?
    release = f"r{str(params.RELEASE)}"

    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    home = expanduser("~")

    # Local Files
    local_files_dir = f"{home}/mafs/mafFilesHold"
    one_big_tsv = f"{home}/mafs/MAF-joinedData.tsv"
    manifest_file = f"{home}/mafs/MAF-manifest.tsv"
    local_pull_list = f"{home}/mafs/MAF-pull_list.tsv"
    file_traversal_list = f"{home}/mafs/MAF-traversal_list.txt"
    field_list = f"{home}/MAF-field_list.json"
    table_metadata = f"{params.SCHEMA_REPO_LOCAL}/{params.SCHEMA_FILE_NAME}"
    metadata_mapping = f"{params.SCHEMA_REPO_LOCAL}/{params.METADATA_MAPPINGS}"
    field_desc_fp = f"{params.SCHEMA_REPO_LOCAL}/{params.FIELD_DESC_FILE}"

    # BigQuery Tables
    manifest_table = f"{params.DATA_TYPE}_manifest_{release}"
    pull_list_table = f"{params.DATA_TYPE}_pull_list_{release}"
    concat_table = f"{params.DATA_TYPE}_concat_{release}"
    barcode_table = f"{params.DATA_TYPE}_barcode_{release}"
    combined_table = f"{params.DATA_TYPE}_combined_table_{release}"
    standard_table = f"{params.DATA_TYPE}_hg38_gdc"
    skel_table_id = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{concat_table}'
    barcodes_table_id = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{barcode_table}'

    draft_table = f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{standard_table}"
    # Google Bucket Locations
    bucket_target_blob = f'{params.WORKING_BUCKET_DIR}/{release}-{params.DATA_TYPE}.tsv'

    # Workflow Steps

    if 'clear_target_directory' in steps:
        # Best practice is to clear out the directory where the files are going. Don't want anything left over:
        create_clean_target(local_files_dir)

    if 'build_manifest' in steps:

        max_files = params.MAX_FILES if 'MAX_FILES' in params else None
        bq_filters = [{"access": "open"},
                      {"data_format": "MAF"},
                      {"data_type": "Masked Somatic Mutation"},
                      {"program_name": params.PROGRAMS}]
        manifest_success = get_the_bq_manifest(params.FILE_TABLE.format(release),
                                               bq_filters, max_files,
                                               params.WORKING_PROJECT, params.SCRATCH_DATASET,
                                               manifest_table, params.WORKING_BUCKET,
                                               params.BUCKET_MANIFEST_TSV, manifest_file,
                                               params.BQ_AS_BATCH)
        if not manifest_success:
            sys.exit("Failure generating manifest")

    if 'build_pull_list' in steps:
        # Create a "pull list" with BigQuery of gs:// URLs to pull from DCF
        build_pull_list_with_bq(f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{manifest_table}",
                                params.INDEXD_BQ_TABLE.format(release),
                                params.WORKING_PROJECT, params.SCRATCH_DATASET,
                                pull_list_table,
                                params.WORKING_BUCKET,
                                params.BUCKET_PULL_LIST,
                                local_pull_list, params.BQ_AS_BATCH)

    if 'transfer_from_gdc' in steps:
        # Bring the files to the local dir from DCF GDC Cloud Buckets
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        pull_from_buckets(pull_list, local_files_dir)

        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write(f"{line}\n")

    if 'concat_all_files' in steps:
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
            concat_all_files(all_files, one_big_tsv, params.CALLERS, params.FIELDS_TO_FIX)

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params.WORKING_BUCKET, bucket_target_blob, one_big_tsv)

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        update_dir_from_git(params.SCHEMA_REPO_LOCAL, params.SCHEMA_REPO_URL, params.SCHEMA_REPO_BRANCH)

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        typing_tups = find_types(one_big_tsv, params.SCHEMA_SAMPLE_SKIPS)

        create_schema_hold_list(typing_tups, field_desc_fp, field_list, True)

    # Create the BQ table from the TSV
    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = f'gs://{params.WORKING_BUCKET}/{bucket_target_blob}'
        with open(field_list, mode='r') as schema_list:
            typed_schema = json_loads(schema_list.read())
        csv_to_bq(typed_schema, bucket_src_url, params.SCRATCH_DATASET, concat_table, params.BQ_AS_BATCH)

    # Merge in aliquot and sample barcodes from other tables
    if 'collect_barcodes' in steps:

        case_table = params.CASE_TABLE.format(release)
        success = attach_barcodes(skel_table_id, params.ALIQUOT_TABLE.format(release),
                                  params.SCRATCH_DATASET, barcode_table, params.BQ_AS_BATCH, case_table)
        if not success:
            print("attach_barcodes job failed")
            return

    # Merge the barcode info into the final combo table we are building:
    if 'create_combo_draft_table' in steps:
        success_barcode = barcode_raw_table_merge(skel_table_id, barcodes_table_id,
                                                  params.SCRATCH_DATASET, combined_table,
                                                  params.BQ_AS_BATCH)
        # Eliminate the duplicates by merging samples by aliquots
        if success_barcode:
            program_draft_table = f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{combined_table}"
            success = merge_samples_by_aliquot(program_draft_table, f"{standard_table}_{release}", params.SCRATCH_DATASET,
                                               params.CALLERS, params.BQ_AS_BATCH)
        else:
            print("Barcode & Raw table merge failed")

        if not success:
            print("Join job failed")
            return

    # Split the merged table into distinct programs and create final draft tables
    for program in params.PROGRAMS:

        program_map = dict()
        with open(metadata_mapping) as program_mapping:
            mappings = json_loads(program_mapping.read().rstrip())
            bq_dataset = mappings[program]['bq_dataset']

        if 'split_table_into_programs' in steps:
            success = create_per_program_table(f"{draft_table}_{release}", f"{bq_dataset}_{standard_table}_{release}", program,
                                               params.SCRATCH_DATASET, params.BQ_AS_BATCH)

            if not success:
                print(f"split table into programs failed on {program}")

        if 'update_table_schema' in steps:
            print("update schema tags")
            updated_schema_tags = update_schema_tags(metadata_mapping, params, program)
            print("update table schema")
            write_table_schema_with_generic(
                f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{bq_dataset}_{standard_table}_{release}",
                updated_schema_tags, table_metadata, field_desc_fp)

        if 'qc_bigquery_tables' in steps:
            print("QC BQ")

        if 'publish' in steps:
            print('Attempting to publish tables')
            success = publish_tables_and_update_schema(f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{bq_dataset}_{standard_table}_{release}",
                                                       f"{params.PUBLICATION_PROJECT}.{bq_dataset}_versioned.{standard_table}_{release}",
                                                       f"{params.PUBLICATION_PROJECT}.{bq_dataset}.{standard_table}_current",
                                                       f"REL {str(params.RELEASE)}",
                                                       f"{standard_table}")

            if not success:
                print("Publication step did not work")

    # Clear out working temp tables:
    if 'dump_working_tables' in steps:
        dump_tables = [concat_table,
                       barcode_table,
                       f"{bq_dataset}_{standard_table}_current",
                       f"{bq_dataset}_{standard_table}_{release}",
                       manifest_table]
        for table in dump_tables:
            if bq_table_exists(params.SCRATCH_DATASET, table, params.WORKING_PROJECT):
                delete_table_bq_job(params.SCRATCH_DATASET, table)

    if 'archive' in steps:
        print('archive files from VM')
        archive_file_prefix = f"{date.today()}_{params.PUBLICATION_DATASET}"
        if params.ARCHIVE_YAML:
            yaml_file = re.search(r"\/(\w*.yaml)$", args[1])
            archive_yaml = f"{params.ARCHIVE_BUCKET_DIR}/{params.ARCHIVE_CONFIG}/{archive_file_prefix}_{yaml_file.group(1)}"
            upload_to_bucket(params.ARCHIVE_BUCKET, archive_yaml, args[1])
        archive_pull_file = f"{params.ARCHIVE_BUCKET_DIR}/{archive_file_prefix}_{params.LOCAL_PULL_LIST}"
        upload_to_bucket(params.ARCHIVE_BUCKET, archive_pull_file, params.LOCAL_PULL_LIST)
        archive_manifest_file = f"{params.ARCHIVE_BUCKET_DIR}/{archive_file_prefix}_{params.MANIFEST_FILE}"
        upload_to_bucket(params.ARCHIVE_BUCKET,
                         archive_manifest_file,
                         params.MANIFEST_FILE)

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
