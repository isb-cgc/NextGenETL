"""
Copyright 2019-2024, Institute for Systems Biology
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
import logging
from gdc_file_utils import query_bq, cluster_table

mirna_isoform_expr_logger = logging.getLogger(name='base_script.mirna_isoform_expr')


def attach_aliquot_ids(input_table, file_table, output_table):
    """
    SQL query for adding GDC file metadata to the table. GDC file UUID for the file
    is pulled from the bucket path. We need aliquots, samples, and associated barcodes.
    We get this by first attaching the aliquot IDs using the file table that provides
    aliquot UUIDs for files.
    :param input_table: Table id with miRNA isoform seq data
    :param file_table: GDC metadata table id with file metadata
    :param output_table: Table id to create
    :return: If the function worked
    """
    sql = f'''
        WITH
        a1 AS (SELECT DISTINCT LEFT(file_name, 36) as file_gdc_id FROM `{input_table}`),        
        a2 AS (SELECT b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
                      a1.file_gdc_id
               FROM a1 JOIN `{file_table}` AS b ON a1.file_gdc_id= b.file_gdc_id
               WHERE b.associated_entities__entity_type = 'aliquot')
        SELECT 
               c.project_short_name,
               c.case_gdc_id,
               c.associated_entities__entity_gdc_id AS aliquot_gdc_id,
               a2.file_gdc_id
        FROM a2 JOIN `{file_table}` AS c ON a2.file_gdc_id = c.file_gdc_id
        WHERE c.associated_entities__entity_type = 'aliquot'       
        '''
    return query_bq(sql, output_table)


def attach_barcodes(input_table, aliquot_table, case_table, output_table):
    """
    Create a table with sample information. DISTINCT was added to the query to avoid duplicates in aliquots when
    tables were joined together.
    :param input_table: Table id with miRNA isoform seq data
    :param aliquot_table: Metadata table id with aliquot data
    :param case_table: Metadata table id with case data
    :param output_table: Table id to create
    :return: If the function worked
    """
    sql = f'''
        WITH a1 AS (
            SELECT DISTINCT 
                a.project_short_name,
                c.case_barcode,
                c.sample_barcode,
                c.aliquot_barcode,
                c.sample_type_name,
                c.case_gdc_id,
                c.sample_gdc_id,
                a.aliquot_gdc_id,
                a.file_gdc_id 
            FROM `{input_table}`as a JOIN `{aliquot_table}` AS c ON a.aliquot_gdc_id = c.aliquot_gdc_id 
            WHERE c.case_gdc_id = a.case_gdc_id)
        SELECT
            a1.project_short_name,
            a1.case_barcode,
            a1.sample_barcode,
            a1.aliquot_barcode,
            b.primary_site,
            a1.sample_type_name,
            a1.case_gdc_id,
            a1.sample_gdc_id,
            a1.aliquot_gdc_id,
            a1.file_gdc_id
        FROM a1 JOIN `{case_table}` as b ON a1.case_barcode = b.case_barcode and a1.project_short_name = b.project_id
        '''

    return query_bq(sql, output_table)


'''
----------------------------------------------------------------------------------------------
Glue the New Info to the Original Table
All the new info we have pulled together goes in the first columns of the final table
'''
def join_barcode_and_raw_tables(input_table, barcode_table, output_table):
    """
    Glue the New Info to the Original Table
    All the new info we have pulled together goes in the first columns of this table.
    :param input_table: Table id with miRNA isoform seq data
    :param barcode_table: Metadata table id with barcode data
    :param output_table: Table id to create
    :return: If the function worked
    """
    sql = f'''
        SELECT
            a.project_short_name,
            a.case_barcode,
            a.sample_barcode,
            a.aliquot_barcode,
            a.primary_site,
            b.miRNA_ID AS miRNA_id,
            REGEXP_EXTRACT(b.isoform_coords, r"\S*:(chr\d*|\w*):") as chromosome,
            CAST(REGEXP_EXTRACT(b.isoform_coords, r"(\d+)-") as INTEGER) as start_pos,
            CAST(REGEXP_EXTRACT(b.isoform_coords, r"-(\d+)") as INTEGER) as end_pos,
            RIGHT(b.isoform_coords, 1) as strand,
            b.read_count,
            b.reads_per_million_miRNA_mapped,
            b.`cross-mapped` as cross_mapped,
            REGEXP_EXTRACT(b.miRNA_region, r"(\w*),*") as mirna_transcript,
            REGEXP_EXTRACT(b.miRNA_region, r",(\S*)") as miRNA_accession,
            a.sample_type_name,
            a.case_gdc_id,
            a.sample_gdc_id,
            a.aliquot_gdc_id,
            LEFT(b.file_name, 36) as file_gdc_id
        FROM `{barcode_table}` as a JOIN `{input_table}` as b ON a.file_gdc_id = LEFT(b.file_name, 36)
        '''

    return query_bq(sql, output_table)


def merge_samples_by_aliquot(input_table, output_table):
    """
    Aggregate sample barcodes and ids based on aliquots
    :param input_table: Combined table id with metadata and miRNA seq data
    :param output_table: Table id to create
    :return: If the function worked
    """
    sql = f'''
    SELECT 
        project_short_name,
        case_barcode,
        string_agg(sample_barcode, ';') as sample_barcode,
        aliquot_barcode,
        primary_site,
        miRNA_id,
        chromosome,
        start_pos,
        end_pos,
        strand,
        read_count,
        reads_per_million_miRNA_mapped,
        cross_mapped,
        miRNA_transcript,
        miRNA_accession,
        sample_type_name,
        case_gdc_id,
        string_agg(sample_gdc_id, ';') as sample_gdc_id,
        aliquot_gdc_id,
        file_gdc_id
    FROM `{input_table}`
    GROUP BY
        project_short_name,
        case_barcode,
        aliquot_barcode,
        primary_site,
        miRNA_id,
        chromosome,
        start_pos,
        end_pos,
        strand,
        read_count,
        reads_per_million_miRNA_mapped,
        cross_mapped,
        miRNA_transcript,
        miRNA_accession,
        sample_type_name,
        case_gdc_id,
        aliquot_gdc_id,
        file_gdc_id'''

    return query_bq(sql, output_table)


def create_mirna_isoform_expr_table(raw_isoform_mirna_expr, draft_mirna_isoform_expr, file_table, aliquot_table,
                            case_table, project_id, dataset, release):
    """
    Run through the SQL queries to create the final draft table.
    :param raw_isoform_mirna_expr: Initial miRNA isoform expression table name
    :param draft_mirna_isoform_expr: Draft miRNA isoform expression table name
    :param file_table: Metadata table with file data
    :param aliquot_table: Metadata table with aliquot data
    :param case_table: Metadata table with case data
    :param project_id: Project of where the tables are to be created
    :param dataset: Dataset of where the tables are to be created
    :param release: GDC release
    :return: list of tables created
    """

    mirna_isoform_expr_logger.info("Creating {draft_mirna_isoform_expr}")

    created_tables = []
    step_1_table = f"{raw_isoform_mirna_expr}_step_1"
    step_2_table = f"{raw_isoform_mirna_expr}_step_2"
    step_3_table = f"{raw_isoform_mirna_expr}_step_3"
    step_4_table = f"{raw_isoform_mirna_expr}_step_4"

    attach_aliquot_ids_results = attach_aliquot_ids(f"{project_id}.{dataset}.{raw_isoform_mirna_expr}",
                                                    f"{file_table}_{release}",
                                                    f"{project_id}.{dataset}.{step_1_table}")

    if attach_aliquot_ids_results == 'DONE':
        created_tables.append(step_1_table)
    else:
        mirna_isoform_expr_logger.error("Creating miRNA Seq aliquot id table failed")
        sys.exit()


    attach_barcodes_results = attach_barcodes(f"{project_id}.{dataset}.{step_1_table}",
                                              f"{aliquot_table}_{release}",
                                              f"{case_table}_{release}",
                                              f"{project_id}.{dataset}.{step_2_table}")
    if attach_barcodes_results == 'DONE':
        created_tables.append(step_2_table)
    else:
        mirna_isoform_expr_logger.error("Creating miRNA Seq aliquot barcode table failed")
        sys.exit()

    join_barcode_and_raw_tables_results = join_barcode_and_raw_tables(f"{project_id}.{dataset}.{raw_isoform_mirna_expr}",
                                      f"{project_id}.{dataset}.{step_2_table}",
                                      f"{project_id}.{dataset}.{step_3_table}")
    if join_barcode_and_raw_tables_results == 'DONE':
        created_tables.append(step_3_table)
    else:
        mirna_isoform_expr_logger.error("Creating miRNA Seq merged barcode table failed")
        sys.exit()


    merge_samples_by_aliquot_results = merge_samples_by_aliquot(f"{project_id}.{dataset}.{step_3_table}",
                                      f"{project_id}.{dataset}.{step_4_table}")
    if merge_samples_by_aliquot_results == 'DONE':
        created_tables.append(step_4_table)
    else:
        mirna_isoform_expr_logger.error("Creating miRNA Seq table failed")
        sys.exit()


    # todo describe
    cluster_fields = ["project_short_name", "case_barcode", "sample_barcode", "aliquot_barcode"] # todo what should we cluster on?
    cluster_table_result = cluster_table(f"{project_id}.{dataset}.{step_4_table}",
                                         f"{project_id}.{dataset}.{draft_mirna_isoform_expr}", cluster_fields)
    if cluster_table_result.total_rows < 1:
        created_tables.append(draft_mirna_isoform_expr)
    else:
        print(cluster_table_result)
        mirna_isoform_expr_logger.error("Creating miRNA Expr draft table failed")
        sys.exit()

    return created_tables
