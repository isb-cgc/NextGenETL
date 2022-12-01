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
import gzip
import shutil
import zipfile
import io
#from git import Repo # todo remove when confirmed not needed
import re
from json import loads as json_loads
from os.path import expanduser
#from createSchemaP3 import build_schema # todo remove when confirmed not needed
from datetime import date
import gzip

# todo remove unused functions from list
from common_etl.support import create_clean_target, pull_from_buckets, build_file_list, generic_bq_harness, \
    upload_to_bucket, csv_to_bq, delete_table_bq_job, \
    build_pull_list_with_bq, write_table_schema_with_generic, update_dir_from_git,\
    build_combined_schema, get_the_bq_manifest, confirm_google_vm, \
    update_schema_tags

from common_etl.utils import find_types, add_generic_table_metadata

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
        return None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['programs'], #yaml_dict['filters'], yaml_dict['bq_filters'],
            yaml_dict['steps'], yaml_dict['callers'], yaml_dict['update_schema_tables'])
            #yaml_dict['schema_tags'])


'''
----------------------------------------------------------------------------------------------
Extract the TCGA Programs We Are Working With From File List
Extract from downloaded file names instead of using a specified list.
'''

# todo remove?
def build_program_list(all_files):
    programs = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        programs.add(info_list[0])

    return sorted(programs)


'''
----------------------------------------------------------------------------------------------
Extract the Callers We Are Working With From File List
Extract from downloaded file names, compare to expected list. Answer if they match.
'''

def check_caller_list(all_files, expected_callers):
    expected_set = set(expected_callers)
    callers = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        callers.add(info_list[1])

    return callers == expected_set


'''
----------------------------------------------------------------------------------------------
First BQ Processing: Add Aliquot IDs
The GDC files only provide tumor and normal BAM file IDS. We need aliquots, samples, and
associated barcodes. We get this by first attaching the aliquot IDs using the file table
that provides aliquot UUIDs for BAM files.
'''


def attach_aliquot_ids(maf_table, file_table, target_dataset, dest_table, do_batch):
    sql = attach_aliquot_ids_sql(maf_table, file_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def attach_aliquot_ids_sql(maf_table, file_table):
    return f"""
        WITH
        a1 AS (SELECT DISTINCT tumor_bam_uuid, normal_bam_uuid FROM `{maf_table}`),        
        a2 AS (SELECT b.associated_entities__entity_gdc_id AS aliquot_gdc_id_tumor,
                      a1.tumor_bam_uuid,
                      a1.normal_bam_uuid
               FROM a1 JOIN `{file_table}` AS b ON a1.tumor_bam_uuid = b.file_gdc_id
               WHERE b.associated_entities__entity_type = 'aliquot')
        SELECT 
               c.project_short_name,
               c.case_gdc_id,
               c.associated_entities__entity_gdc_id AS aliquot_gdc_id_normal,
               a2.aliquot_gdc_id_tumor,
               a2.tumor_bam_uuid,
               a2.normal_bam_uuid
        FROM a2 JOIN `{file_table}` AS c ON a2.normal_bam_uuid = c.file_gdc_id
        WHERE c.associated_entities__entity_type = 'aliquot'
        """


'''
----------------------------------------------------------------------------------------------
Second BQ Processing: Add Barcodes
With the aliquot UUIDs known, we can now use the aliquot table to glue in sample info
'''


def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch, case_table):
    sql = attach_barcodes_sql(temp_table, aliquot_table, case_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def attach_barcodes_sql(maf_table, aliquot_table, case_table):
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


'''
----------------------------------------------------------------------------------------------
Final BQ Step: Glue the New Info to the Original Table
All the new info we have pulled together goes in the first columns of the final table
'''


def barcode_raw_table_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch):
    sql = final_join_sql(maf_table, barcode_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def final_join_sql(maf_table, barcodes_table):
    # if program == 'TCGA':
    #     return '''
    #          SELECT a.project_short_name,
    #                 a.case_barcode,
    #                 b.*,
    #                 a.sample_barcode_tumor,
    #                 a.sample_barcode_normal,
    #                 a.aliquot_barcode_tumor,
    #                 a.aliquot_barcode_normal,
    #          FROM `{0}` as a JOIN `{1}` as b ON a.tumor_bam_uuid = b.tumor_bam_uuid
    #     '''.format(barcodes_table, maf_table)
    # else:
    return f"""
         SELECT a.project_short_name,
                a.case_barcode,
                a.primary_site,
                b.*,
                a.sample_barcode_tumor,
                a.sample_barcode_normal,
                a.aliquot_barcode_tumor, 
                a.aliquot_barcode_normal,
         FROM `{barcodes_table}` as a JOIN `{maf_table}` as b 
         ON a.aliquot_gdc_id_tumor = b.Tumor_Aliquot_UUID AND a.Start_Position = b.Start_Position AND a.Chromosome = b.Chromosome
    """


'''
----------------------------------------------------------------------------------------------
file_info() function Author: Sheila Reynolds
File name includes important information, e.g. the program name and the caller. Extract that
out along with name and ID.
'''


def file_info(aFile):
    norm_path = os.path.normpath(aFile)
    path_pieces = norm_path.split(os.sep)

    # if program == 'TCGA': # todo remove TCGA
    #     file_name = path_pieces[-1]
    #     file_name_parts = file_name.split('.')
    #     callerName = file_name_parts[2]
    #     fileUUID = file_name_parts[3]
    # else:
    fileUUID = path_pieces[-2]
    callerName = None

    return ([callerName, fileUUID])


'''
------------------------------------------------------------------------------
Clean header field names
Some field names are not accurately named and as of 2020-08-05, the GDC has said they will not be updated. We decided to 
update the field names to accurately reflect the data within th column. As of GDC r32, the columns are still named
incorrectly.
'''

def clean_header_names(header_list, fields_to_fix):
    # todo remove commented out lines
    #header_id = header_line.split('\t')
    # if program != 'TCGA':
    for header_name in range(len(header_list)):
        for dict in fields_to_fix:
            original, new = next(iter(dict.items()))

            if header_list[header_name] == original:
                header_list[header_name] = new

    return header_list


'''
------------------------------------------------------------------------------
Separate the Callers into their own columns
The maf files has one column with a semicolon delimited with the callers in it.
'''


def process_callers(callers_str, callers):
    """
    # todo
    :param callers_str:
    :type callers_str:
    :param callers:
    :type callers:
    :return:
    :rtype:
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


'''
------------------------------------------------------------------------------
Concatenate all Files
'''


def concat_all_files(all_files, one_big_tsv, callers, fields_to_fix):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
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
                # dir_name = os.path.dirname(filename)
                use_file_name = filename[:-3]
                print(f"Uncompressing {filename}")
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename
            with open(use_file_name, 'r') as readfile:
                callerName, fileUUID = file_info(use_file_name)
                # caller_field_index = 0
                # print(str(caller_field_index))
                for line in readfile:
                    # Seeing comments in MAF files
                    if not line.startswith('#'):
                        if first:
                            header_list = line.rstrip('\n').split('\t')
                            header_id = header_list[0]
                            header_names = clean_header_names(header_list, fields_to_fix)
                            caller_field_index = header_names.index('callers')
                            header_line = '\t'.join(header_names)
                            outfile.write(header_line) #.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write('file_gdc_id')
                            # todo remove
                            # if program == "TCGA":
                            #     outfile.write('\t')
                            #     outfile.write('caller')
                            # else:
                            for field in callers:
                                outfile.write('\t')
                                outfile.write(field)
                            outfile.write('\n')
                            first = False
                        if not line.startswith(header_id):
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write(fileUUID)
                            # todo remove
                            # if program == "TCGA":
                            #     outfile.write('\t')
                            #     outfile.write(callerName)
                            # else:
                            caller_data = process_callers(line.rstrip('\n').split('\t')[caller_field_index], callers)
                            for caller in callers:
                                outfile.write('\t')
                                outfile.write(caller_data[caller])
                            outfile.write('\n')
                if toss_zip:
                    os.remove(use_file_name)


def merge_samples_by_aliquot(input_table, output_table, target_dataset, do_batch):
    sql = merge_samples_by_aliquot_sql(input_table)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')


def merge_samples_by_aliquot_sql(input_table): # todo fix to grab callers from yaml
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
        muse,
        mutect2,
        pindel,
        somaticsniper,
        varscan2,
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
        muse,
        mutect2,
        pindel,
        somaticsniper,
        varscan2,
        aliquot_barcode_tumor, 
        aliquot_barcode_normal"""


def create_per_program_table(input_table, output_table, program, target_dataset, do_batch):
    sql = sql_create_per_program_table(input_table, program)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')


def sql_create_per_program_table(input_table, program):
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
    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, programs, steps, callers, update_schema_tables = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    # Which table are we building?
    release = f"r{str(params['RELEASE'])}"

    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    home = expanduser("~")

    # Local Files
    local_files_dir = f"{home}/{params['LOCAL_FILES_DIR']}"
    one_big_tsv = f"{home}/{params['ONE_BIG_TSV']}"
    manifest_file = f"{home}/{params['MANIFEST_FILE']}"
    local_pull_list = f"{home}/{params['LOCAL_PULL_LIST']}"
    file_traversal_list = f"{home}/{params['FILE_TRAVERSAL_LIST']}"
    hold_schema_list = f"{home}/{params['HOLD_SCHEMA_LIST']}"  # todo rename to appropriate file
    hold_schema_dict = f"{home}/{params['HOLD_SCHEMA_DICT']}"  # todo rename to appropriate file
    table_metadata = f"{home}/{params['SCHEMA_FILE_NAME']}"
    metadata_mapping = f"{home}/{params['METADATA_MAPPINGS']}" # todo add to yaml
    field_desc_fp = f"{home}/params['FIELD_DESC_FILE']"

    # BigQuery Tables
    manifest_table = f"{params['DATA_TYPE']}_manifest_r{params['RELEASE']}"
    concat_table = f"{params['DATA_TYPE']}_concat_r{params['RELEASE']}"
    barcode_table = f"{params['DATA_TYPE']}_barcode_r{params['RELEASE']}"
    standard_table = f"{params['DATA_TYPE']}_hg38_gdc_r{params['RELEASE']}" # todo should this have the release?
    skel_table_id = f'{params["WORKING_PROJECT"]}.{params["SCRATCH_DATASET"]}.{concat_table}'
    barcodes_table_id = f'{params["WORKING_PROJECT"]}.{params["SCRATCH_DATASET"]}.{barcode_table}'
    final_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{standard_table}_{release}" # todo rename to accurately reflect the table

    # Google Bucket Locations
    bucket_target_blob = f'{params["WORKING_BUCKET_DIR"]}/{params["DATE"]}-{params["DATA_TYPE"]}.tsv'

    # Which metadata release should we use? # todo do we need this?
    metadata_rel = f"r{str(params['METADATA_REL'])}" if 'METADATA_REL' in params else release

    # Workflow Steps

    if 'clear_target_directory' in steps:
        # Best practice is to clear out the directory where the files are going. Don't want anything left over:
        create_clean_target(local_files_dir)

    if 'build_manifest' in steps:

        # todo add a count for
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None
        bq_filters = [{"access": "open"},
                      {"data_format": "MAF"},
                      {"data_type": "Masked Somatic Mutation"},
                      {"program_name": programs}]
        manifest_success = get_the_bq_manifest(params['FILE_TABLE'].format(metadata_rel),
                                               bq_filters, max_files,
                                               params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                               manifest_table, params['WORKING_BUCKET'],
                                               params['BUCKET_MANIFEST_TSV'], manifest_file,
                                               params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return

    if 'build_pull_list' in steps:
        # Create a "pull list" with BigQuery of gs:// URLs to pull from DCF
        build_pull_list_with_bq(f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{manifest_table}",
                                params['INDEXD_BQ_TABLE'].format(metadata_rel),
                                params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                f"{params['DATA_TYPE']}_pull_list",
                                params['WORKING_BUCKET'],
                                params['BUCKET_PULL_LIST'],
                                local_pull_list, params['BQ_AS_BATCH'])

    if 'transfer_from_gdc' in steps:
        # Bring the files to the local dir from DCF GDC Cloud Buckets
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        pull_from_buckets(pull_list, local_files_dir)

    if 'build_traversal_list' in steps:
        # Traverse the tree of downloaded files and create a flat list of all files:
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write(f"{line}\n")

    if 'concat_all_files' in steps:
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
            concat_all_files(all_files, one_big_tsv, callers, params['FIELDS_TO_FIX'])

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, one_big_tsv)

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        update_dir_from_git(params['SCHEMA_REPO_LOCAL'], params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_BRANCH'])

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        typing_tups = find_types(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        # full_file_prefix = f"{params['PROX_DESC_PREFIX']}/{draft_table}_{release}"
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list, hold_schema_dict)

    # Create the BQ table from the TSV
    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = f'gs://{params["WORKING_BUCKET"]}/{bucket_target_blob}'
        with open(hold_schema_list, mode='r') as schema_list:
            typed_schema = json_loads(schema_list.read())
        csv_to_bq(typed_schema, bucket_src_url, params['SCRATCH_DATASET'], concat_table, params['BQ_AS_BATCH'])

    # Merge in aliquot and sample barcodes from other tables
    if 'collect_barcodes' in steps:

        case_table = params['CASE_TABLE'].format(release)
        success = attach_barcodes(skel_table_id, params['ALIQUOT_TABLE'].format(release),
                                  params['SCRATCH_DATASET'], barcode_table, params['BQ_AS_BATCH'], case_table)
        if not success:
            print("attach_barcodes job failed")
            return

    # Merge the barcode info into the final combo table we are building:
    if 'create_final_combo_table' in steps: # todo rename
        success_barcode = barcode_raw_table_merge(skel_table_id, barcodes_table_id,
                                                  params['SCRATCH_DATASET'], f"{standard_table}_combined_table",
                                                  params['BQ_AS_BATCH'])
        # Eliminate the duplicates by merging samples by aliquots
        if success_barcode:
            release_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{standard_table}_combined_table" # todo rename
            success = merge_samples_by_aliquot(release_table, f"{standard_table}_{release}", params['SCRATCH_DATASET'], # todo rename
                                               params['BQ_AS_BATCH'])
        else:
            print("Barcode & Raw table merge failed")

        if not success:
            print("Join job failed")
            return

    # Split the merged table into distinct programs and create final draft tables
    for program in programs:

        release_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{program}_{standard_table}_{release}" # todo change name
        current_dest = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{program}_{standard_table}_current"

        program_map = dict()
        with open(metadata_mapping) as program_mapping:
            mappings = json_loads(program_mapping)
            program_map[program] = mappings[program]['bq_dataset']

        if 'split_table_into_programs' in steps:
            success = create_per_program_table(final_table, f"{program_map[program]}_{standard_table}", program, params['SCRATCH_DATASET'], metadata_mapping, params['BQ_AS_BATCH'])

            if not success:
                print(f"split table into programs failed on {program}")

        if 'update_table_schema' in steps:
            updated_schema_tags = update_schema_tags(metadata_mapping, params, program)
            write_table_schema_with_generic(program, standard_table, updated_schema_tags, metadata_mapping, table_metadata, field_desc_fp) # todo make sure it has the correct mapping

        # if 'publish' in steps: # todo wrap in a common function?
        #     print('Attempting to publish tables')
        #     full_scratch_versioned = f'{params["WORKING_PROJECT"]}.{params["TARGET_DATASET"]}.{versioned_scratch_table}'  # todo already defined?
        #
        #     placeholder(full_scratch_versioned, params['PUBLISH_ONLY_UPDATED']) # todo update

        # Clear out working temp tables:
        if 'dump_working_tables' in steps:
            dump_tables = [concat_table,
                           barcode_table,
                           f"{standard_table}_current",
                           f"{standard_table}_{release}",
                           manifest_table]
            for table in dump_tables:
                delete_table_bq_job(params['SCRATCH_DATASET'], table)

    #
    # Done!
    #

    print('job completed')

    if 'archive' in steps:

        print('archive files from VM')
        archive_file_prefix = f"{date.today()}_{params['PUBLICATION_DATASET']}"
        if params['ARCHIVE_YAML']:
            yaml_file = re.search(r"\/(\w*.yaml)$", args[1])
            archive_yaml = f""""{params['ARCHIVE_BUCKET_DIR']}/
                                {params['ARCHIVE_CONFIG']}/
                                {archive_file_prefix}_{yaml_file.group(1)}"""
            upload_to_bucket(params['ARCHIVE_BUCKET'],
                             archive_yaml,
                             args[1])
        archive_pull_file = f"{params['ARCHIVE_BUCKET_DIR']}/{archive_file_prefix}_{params['LOCAL_PULL_LIST']}"
        upload_to_bucket(params['ARCHIVE_BUCKET'],
                         archive_pull_file,
                         params['LOCAL_PULL_LIST'])
        archive_manifest_file = f"{params['ARCHIVE_BUCKET_DIR']}/{archive_file_prefix}_{params['MANIFEST_FILE']}"
        upload_to_bucket(params['ARCHIVE_BUCKET'],
                         archive_manifest_file,
                         params['MANIFEST_FILE'])


if __name__ == "__main__":
    main(sys.argv)
