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

def collect_aliquot_and_file_info(raw_table, file_table, aliquot_table, raw_sample_table, output_table):
    """
    SQL Query to add file and aliquot info. This query adds a column for normal and tumor.

    :param raw_table: Raw Gene Level Copy Number table name
    :type raw_table: basestring
    :param file_table: File Metadata Table
    :type file_table: basestring
    :param aliquot_table: Aliquot Metadata table name
    :type aliquot_table: basestring
    :param raw_sample_table: Raw Gene Level Copy Number table name
    :param output_table: Output table name
    :return: If the function worked
    :rtype: basestring
    """

    regex_string1 = r"^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}"
    regex_string2 = r"\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$"

    sql = f"""
        WITH
          tumor_table AS (
          SELECT
            b.project_id AS project_short_name,
            b.case_barcode,
            STRING_AGG(DISTINCT b.sample_barcode, ";") AS tumor_sample_barcode,
            STRING_AGG(DISTINCT b.aliquot_barcode, ";") AS tumor_aliquot_barcode,
            b.case_gdc_id,
            STRING_AGG(DISTINCT b.sample_gdc_id, ";") AS tumor_sample_gdc_id,
            e.tissue_type AS tumor_tissue_type,
            e.tumor_descriptor AS tumor_tumor_descriptor,
            STRING_AGG(DISTINCT b.aliquot_gdc_id, ";") AS tumor_aliquot_gdc_id,
            a.file_gdc_id
          FROM
            `{file_table}` AS a
          JOIN
            `{aliquot_table}` AS b
          ON
            REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r"^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}") = b.aliquot_gdc_id
            OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r"\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$") = b.aliquot_gdc_id
          JOIN
            `{raw_table}` AS d
          ON
            a.file_gdc_id = LEFT(d.file_name, 36)
          JOIN
            `{raw_sample_table}` AS e
          ON
            b.sample_gdc_id = e.sample_id
          WHERE
            b.sample_type_name <> "Granulocytes"
            AND `access` = "open"
            AND a.data_type = "Gene Level Copy Number"
            AND a.data_category = "Copy Number Variation"
            AND e.tissue_type != "Normal"
          GROUP BY
            project_short_name,
            case_barcode,
            case_gdc_id,
            tumor_tissue_type,
            tumor_tumor_descriptor,
            file_gdc_id),
          normal_table AS (
          SELECT
            b.case_barcode,
            STRING_AGG(DISTINCT b.sample_barcode, ";") AS normal_sample_barcode,
            STRING_AGG(DISTINCT b.aliquot_barcode, ";") AS normal_aliquot_barcode,
            b.case_gdc_id,
            STRING_AGG(DISTINCT b.sample_gdc_id, ";") AS normal_sample_gdc_id,
            e.tissue_type AS normal_tissue_type,
            e.tumor_descriptor AS normal_tumor_descriptor,
            STRING_AGG(DISTINCT b.aliquot_gdc_id, ";") AS normal_aliquot_gdc_id,
            a.file_gdc_id
          FROM
            `{file_table}` AS a
          JOIN
            `{aliquot_table}` AS b
          ON
            REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r"^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}") = b.aliquot_gdc_id
            OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r"\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$") = b.aliquot_gdc_id
          JOIN
            `{raw_table}` AS d
          ON
            a.file_gdc_id = LEFT(d.file_name, 36)
          JOIN
            `{raw_sample_table}` AS e
          ON
            b.sample_gdc_id = e.sample_id
          WHERE
            b.sample_type_name <> "Granulocytes"
            AND `access` = "open"
            AND a.data_type = "Gene Level Copy Number"
            AND a.data_category = "Copy Number Variation"
            AND e.tissue_type = "Normal"
          GROUP BY
            project_short_name,
            case_barcode,
            case_gdc_id,
            normal_tissue_type,
            normal_tumor_descriptor,
            file_gdc_id)
        SELECT
          f.project_short_name,
          f.case_barcode,
          f.tumor_sample_barcode,
          g.normal_sample_barcode,
          f.tumor_aliquot_barcode,
          g.normal_aliquot_barcode,
          f.case_gdc_id,
          f.tumor_sample_gdc_id,
          g.normal_sample_gdc_id,
          f.tumor_tissue_type,
          g.normal_tissue_type,
          f.tumor_tumor_descriptor,
          g.normal_tumor_descriptor,
          f.tumor_aliquot_gdc_id,
          g.normal_aliquot_gdc_id,
          f.file_gdc_id
        FROM
          tumor_table AS f
        LEFT JOIN
          normal_table AS g
        ON
          f.file_gdc_id = g.file_gdc_id
    """

    #     WITH
    #       tumor_table AS (
    #       SELECT
    #         b.project_id AS project_short_name,
    #         b.case_barcode,
    #         b.sample_barcode AS tumor_sample_barcode,
    #         b.aliquot_barcode AS tumor_aliquot_barcode,
    #         b.case_gdc_id,
    #         b.sample_gdc_id AS tumor_sample_gdc_id,
    #         e.tissue_type AS tumor_tissue_type,
    #         e.tumor_descriptor AS tumor_tissue_descriptor,
    #         b.aliquot_gdc_id AS tumor_aliquot_gdc_id,
    #         a.file_gdc_id
    #       FROM
    #         `{file_table}` AS a
    #       JOIN
    #         `{aliquot_table}` AS b
    #       ON
    #           REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string1}') = b.aliquot_gdc_id
    #           OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string2}') = b.aliquot_gdc_id
    #       JOIN
    #         `{raw_table}` AS d
    #       ON
    #         a.file_gdc_id = LEFT(d.file_name, 36)
    #       JOIN
    #         `{raw_sample_table}` AS e
    #       ON
    #         b.sample_gdc_id = e.sample_id
    #       WHERE
    #         b.sample_type_name <> "Granulocytes"
    #         AND `access` = "open"
    #         AND a.data_type = "Gene Level Copy Number"
    #         AND a.data_category = "Copy Number Variation"
    #         AND e.tissue_type != "Normal"),
    #       normal_table AS (
    #       SELECT
    #         b.case_barcode,
    #         b.sample_barcode AS normal_sample_barcode,
    #         b.aliquot_barcode AS normal_aliquot_barcode,
    #         b.case_gdc_id,
    #         b.sample_gdc_id AS normal_sample_gdc_id,
    #         e.tissue_type AS normal_tissue_type,
    #         e.tumor_descriptor AS normal_tissue_descriptor,
    #         b.aliquot_gdc_id AS normal_aliquot_gdc_id,
    #         a.file_gdc_id
    #       FROM
    #         `{file_table}` AS a
    #       JOIN
    #         `{aliquot_table}` AS b
    #       ON
    #           REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string1}') = b.aliquot_gdc_id
    #           OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string2}') = b.aliquot_gdc_id
    #       JOIN
    #         `{raw_table}` AS d
    #       ON
    #         a.file_gdc_id = LEFT(d.file_name, 36)
    #       JOIN
    #         `{raw_sample_table}` AS e
    #       ON
    #         b.sample_gdc_id = e.sample_id
    #       WHERE
    #         b.sample_type_name <> "Granulocytes"
    #         AND `access` = "open"
    #         AND a.data_type = "Gene Level Copy Number"
    #         AND a.data_category = "Copy Number Variation"
    #         AND e.tissue_type = "Normal")
    #     SELECT
    #       f.project_short_name,
    #       f.case_barcode,
    #       STRING_AGG(DISTINCT f.tumor_sample_barcode, ';') AS tumor_sample_barcode,
    #       STRING_AGG(DISTINCT g.normal_sample_barcode, ';') AS normal_sample_barcode,
    #       STRING_AGG(DISTINCT f.tumor_aliquot_barcode, ';') AS tumor_aliquot_barcode,
    #       STRING_AGG(DISTINCT g.normal_aliquot_barcode, ';') AS normal_aliquot_barcode,
    #       f.case_gdc_id,
    #       STRING_AGG(DISTINCT f.tumor_sample_gdc_id, ';') AS tumor_sample_gdc_id,
    #       STRING_AGG(DISTINCT g.normal_sample_gdc_id, ';') AS normal_sample_gdc_id,
    #       f.tumor_tissue_type,
    #       g.normal_tissue_type,
    #       f.tumor_tissue_descriptor,
    #       g.normal_tissue_descriptor,
    #       STRING_AGG(DISTINCT f.tumor_aliquot_gdc_id, ';') AS tumor_aliquot_gdc_id,
    #       STRING_AGG(DISTINCT g.normal_aliquot_gdc_id, ';') AS normal_aliquot_gdc_id,
    #       f.file_gdc_id
    #     FROM
    #       tumor_table AS f
    #     LEFT JOIN
    #       normal_table AS g
    #     ON
    #       f.file_gdc_id = g.file_gdc_id
    #     GROUP BY
    #       project_short_name,
    #       case_barcode,
    #       case_gdc_id,
    #       tumor_tissue_type,
    #       normal_tissue_type,
    #       tumor_tissue_descriptor,
    #       normal_tissue_descriptor,
    #       file_gdc_id
    # """

    return query_bq(sql, output_table)

def add_case_aliquot_data(raw_data_table, file_aliquot_table, output_table, case_table):
    """
    This SQL combines the case and aliquot data to the raw data table.
    :param raw_data_table: Table id with the gene level copy number data.
    :param file_aliquot_table: Case and aliquot data for the gene level copy number data.
    :param output_table: Table id to create
    :param case_table: Metadata table id with case data
    :return:
    """

    sql = f"""
        SELECT
          a.project_short_name,
          a.case_barcode,
          primary_site,
          a.tumor_sample_barcode,
          a.normal_sample_barcode,
          a.tumor_aliquot_barcode,
          a.normal_aliquot_barcode,
          c.gene_id AS Ensembl_gene_id_v,
          c.gene_name,
          c.chromosome,
          c.start AS start_pos,
          c.`end` AS end_pos,
          c.copy_number,
          c.min_copy_number,
          c.max_copy_number,
          a.case_gdc_id,
          a.tumor_sample_gdc_id,
          a.normal_sample_gdc_id,
          a.tumor_tissue_type,
          a.normal_tissue_type,
          a.tumor_tissue_descriptor,
          a.normal_tissue_descriptor,
          a.tumor_aliquot_gdc_id,
          a.normal_aliquot_gdc_id,
          a.file_gdc_id
        FROM
          `{file_aliquot_table}` AS a
        JOIN
          `{case_table}` AS b
        ON
          a.case_gdc_id = b.case_gdc_id
        JOIN
          `{raw_data_table}` AS c
        ON
          a.file_gdc_id = LEFT(c.file_name, 36)
     """

    return query_bq(sql, output_table)

def add_gene_info(input_table, gene_info_table, output_table):
    """
    Add the Gencode gene information to create the final table.
    :param input_table:
    :param gene_info_table:
    :param output_table:
    :return:
    """
    sql = f"""
        WITH
            gene_id_table
            AS(
                SELECT
            DISTINCT
            gene_id_v, gene_id, gene_type
            FROM
            `{gene_info_table}`
            )
            SELECT
            a.project_short_name,
            a.case_barcode,
            a.primary_site,
            a.tumor_sample_barcode,
            a.normal_sample_barcode,
            a.tumor_aliquot_barcode,
            a.normal_aliquot_barcode,
            b.gene_id as Ensembl_gene_id,
            a.Ensembl_gene_id_v,
            a.gene_name,
            b.gene_type,
            a.chromosome,
            a.start_pos,
            a.end_pos,
            a.copy_number,
            a.min_copy_number,
            a.max_copy_number,
            a.case_gdc_id,
            a.tumor_sample_gdc_id,
            a.normal_sample_gdc_id,
            a.tumor_tissue_type,
            a.normal_tissue_type,
            a.tumor_tissue_descriptor,
            a.normal_tissue_descriptor,
            a.tumor_aliquot_gdc_id,
            a.normal_aliquot_gdc_id,
            a.file_gdc_id
        FROM
        `{input_table}` as a
        JOIN
        `{gene_info_table}` as b
        ON
        a.Ensembl_gene_id_v = b.gene_id_v
    """

    return query_bq(sql, output_table)

def create_gene_level_cnvr_table(raw_gene_level_cnvr, draft_gene_level_cnvr, file_table, aliquot_table, case_table,
                                 raw_gdc_table, gene_table, project_id, dataset, release):
    """
    Run through the SQL queries to create the final draft table.
    :param raw_gene_level_cnvr: Initial copy number gene level table name
    :param draft_gene_level_cnvr: Draft copy number gene level table name
    :param file_table: Metadata table with file data
    :param aliquot_table: Metadata table with aliquot data
    :param case_table: Metadata table with case data
    :param raw_gdc_table: Metadata table with raw case data
    :param gene_table: Metadata table with gene data
    :param project_id: Project of where the tables are to be created
    :param dataset: Dataset of where the tables are to be created
    :param release: GDC release
    :return: list of tables created
    """
    gene_level_cnvr_logger.info(f"Creating {draft_gene_level_cnvr}")

    created_tables = []
    step_1_table = f"{raw_gene_level_cnvr}_step_1"
    step_2_table = f"{raw_gene_level_cnvr}_step_2"
    step_3_table = f"{raw_gene_level_cnvr}_step_3"


    sql_for_aliquot_and_file_table_results = collect_aliquot_and_file_info(f"{project_id}.{dataset}.{raw_gene_level_cnvr}",
                                                    f"{file_table}_{release}",
                                                    f"{aliquot_table}_{release}",
                                                    f"{raw_gdc_table}.{release}_sample",
                                                    f"{project_id}.{dataset}.{step_1_table}")
    if sql_for_aliquot_and_file_table_results == 'DONE':
        created_tables.append(step_1_table)
    else:
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level intermediate table 1 failed")
        sys.exit()

    sql_for_adding_case_aliquot_table_results = add_case_aliquot_data(f"{project_id}.{dataset}.{raw_gene_level_cnvr}",
                                                    f"{file_table}_{release}",
                                                    f"{project_id}.{dataset}.{step_2_table}",
                                                    f"{case_table}_{release}"
                                                    )
    if sql_for_adding_case_aliquot_table_results == 'DONE':
        created_tables.append(step_2_table)
    else:
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level intermediate table 2 failed")
        sys.exit()

    sql_for_add_gene_info_results = add_gene_info(f"{project_id}.{dataset}.{step_2_table}",
                                                    f"{gene_table}",
                                                    f"{project_id}.{dataset}.{step_3_table}"
                                                    )
    if sql_for_add_gene_info_results == 'DONE':
        created_tables.append(step_3_table)
    else:
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level intermediate table 3 failed")
        sys.exit()

    cluster_fields = ["project_short_name", "case_barcode", "sample_barcode", "aliquot_barcode"]
    if bq_table_exists(f"{project_id}.{dataset}.{draft_gene_level_cnvr}"):
        delete_bq_table(f"{dataset}.{draft_gene_level_cnvr}", project=project_id)
    cluster_table_result = cluster_table(f"{project_id}.{dataset}.{step_3_table}",
                                         f"{project_id}.{dataset}.{draft_gene_level_cnvr}", cluster_fields)
    if cluster_table_result.total_rows < 1:
        created_tables.append(draft_gene_level_cnvr)
    else:
        print(cluster_table_result)
        gene_level_cnvr_logger.error("Creating Copy Number Gene Level draft table failed")
        sys.exit()

    return created_tables

