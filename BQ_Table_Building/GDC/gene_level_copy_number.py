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
from gdc_file_utils import query_bq, cluster_table, bq_table_exists, delete_bq_table

gene_level_cnvr_logger = logging.getLogger(name='base_script.gene_level_cnvr')

def sql_for_draft_table(raw_table, file_table, aliquot_table, case_table, gene_table, output_table):
    """
    SQL query to create a final draft table of the copy number gene level data

    :param raw_table: Raw Gene Level Copy Number table name
    :type raw_table: basestring
    :param file_table: File Metadata Table
    :type file_table: basestring
    :param aliquot_table: Aliquot Metadata table name
    :type aliquot_table: basestring
    :param case_table: Case Metadata table name
    :type case_table: basestring
    :param gene_table: GENCODE gene table name
    :type gene_table: basestring
    :param program: GDC Program
    :type program: basestring
    :return: Sting with query to join the tables together
    :rtype: basestring
    """

    regex_string1 = r"^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}"
    regex_string2 = r"\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$"

    sql = f'''
            SELECT
              DISTINCT
              b.project_id AS project_short_name,
              b.case_barcode,
              c.primary_site,
              string_agg(distinct b.sample_barcode, ';') as sample_barcode,
              b.aliquot_barcode,
              e.gene_id as Ensembl_gene_id,
              d.gene_id as Ensembl_gene_id_v,
              d.gene_name,
              e.gene_type,
              d.chromosome,
              d.start AS start_pos,
              d.`end` AS end_pos,
              d.copy_number,
              d.min_copy_number,
              d.max_copy_number,
              b.case_gdc_id,
              string_agg(distinct b.sample_gdc_id, ';') as sample_gdc_id,
              b.aliquot_gdc_id,
              a.file_gdc_id
            FROM
              `{file_table}` AS a
            JOIN
              `{aliquot_table}` AS b
            ON
              REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string1}') = b.aliquot_gdc_id
              OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string2}') = b.aliquot_gdc_id
            JOIN
              `{case_table}` AS c
            ON
              b.case_gdc_id = c.case_gdc_id
            JOIN
              `{raw_table}` AS d
            ON
              a.file_gdc_id = d.source_file_id
            JOIN `{gene_table}` as e
            ON d.gene_id = e.gene_id_v
            WHERE
              b.sample_type_name NOT LIKE '%Normal%'
              AND b.sample_type_name <> "Granulocytes"
              AND `access` = "open"
              AND a.data_type = "Gene Level Copy Number"
              AND a.data_category = "Copy Number Variation"
            GROUP BY
              project_short_name,
              b.case_barcode,
              c.primary_site,
              b.aliquot_barcode,
              Ensembl_gene_id,
              Ensembl_gene_id_v,
              d.gene_name,
              e.gene_type,
              d.chromosome,
              start_pos,
              end_pos,
              d.copy_number,
              d.min_copy_number,
              d.max_copy_number,
              b.case_gdc_id,
              b.aliquot_gdc_id,
              a.file_gdc_id
    '''

    return query_bq(sql, output_table)

def create_gene_level_cnvr_table(raw_gene_level_cnvr, draft_gene_level_cnvr, file_table, aliquot_table, case_table,
                                 gene_table, project_id, dataset, release):
    """
    Run through the SQL queries to create the final draft table.
    :param raw_gene_level_cnvr_seq: Initial copy number gene level table name
    :param draft_gene_level_cnvr: Draft copy number gene level table name
    :param file_table: Metadata table with file data
    :param aliquot_table: Metadata table with aliquot data
    :param case_table: Metadata table with case data
    :param gene_table: Metadata table with gene data
    :param project_id: Project of where the tables are to be created
    :param dataset: Dataset of where the tables are to be created
    :param release: GDC release
    :return: list of tables created
    """
    gene_level_cnvr_logger.info("Creating {draft_gene_level_cnvr}")

    created_tables = []
    step_1_table = f"{raw_gene_level_cnvr}_step_1"
    step_2_table = f"{raw_gene_level_cnvr}_step_2"

    sql_for_draft_table_results = sql_for_draft_table(f"{project_id}.{dataset}.{raw_gene_level_cnvr}",
                                                    f"{file_table}_{release}",
                                                    aliquot_table, case_table, gene_table, step_1_table)
    if sql_for_draft_table_results == 'DONE':
        created_tables.append(step_1_table)
    else:
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level aliquot id table failed")
        sys.exit()

    cluster_fields = ["project_short_name", "case_barcode", "sample_barcode", "aliquot_barcode"]
    if bq_table_exists(f"{project_id}.{dataset}.{draft_gene_level_cnvr}"):
        delete_bq_table(f"{dataset}.{draft_gene_level_cnvr}", project=project_id)
    cluster_table_result = cluster_table(f"{project_id}.{dataset}.{step_2_table}",
                                         f"{project_id}.{dataset}.{draft_gene_level_cnvr}", cluster_fields)
    if cluster_table_result.total_rows < 1:
        created_tables.append(draft_gene_level_cnvr)
    else:
        print(cluster_table_result)
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level draft table failed")
        sys.exit()

    return created_tables

