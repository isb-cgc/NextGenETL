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

rna_seq_logger = logging.getLogger(name='base_script.rna_seq')


def gather_aliquot_ids(input_table, file_table, output_table):
    # todo Is this actually gather file data?
    sql = f'''
        WITH a1 AS (SELECT DISTINCT LEFT(file_name, 36) as file_gdc_id
                FROM `{input_table}`)
        SELECT b.project_short_name,
               b.case_gdc_id,
               b.analysis_input_file_gdc_ids,
               b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               b.platform
        FROM a1 JOIN `{file_table}` AS b ON a1.file_gdc_id = b.file_gdc_id
        WHERE b.associated_entities__entity_type = 'aliquot' '''

    return query_bq(sql, output_table)


def add_barcodes_to_aliquot(step_2_table, aliquot_table, case_table, step_3_table):
    # todo Is this actually gathering case data? why are we bringing in the aliquot table again?
    sql = f'''
        WITH a1 AS (
          SELECT a.project_short_name,
                 b.case_barcode,
                 b.sample_barcode,
                 b.aliquot_barcode,
                 a.case_gdc_id,
                 b.sample_gdc_id,
                 a.aliquot_gdc_id,
                 b.sample_type_name,
                 a.file_gdc_id,
                 a.platform,
                 a.file_name
            FROM `{step_2_table}` AS a 
            JOIN (SELECT DISTINCT * 
                  FROM (SELECT * EXCEPT (analyte_gdc_id, portion_gdc_id) FROM `{aliquot_table}`) ) AS b 
                    ON a.aliquot_gdc_id = b.aliquot_gdc_id)
        SELECT a1.project_short_name,
               a1.case_barcode,
               c.primary_site,
               a1.sample_type_name,
               a1.case_gdc_id,
               a1.sample_barcode,
               a1.aliquot_barcode,
               a1.sample_gdc_id,
               a1.aliquot_gdc_id,
               a1.file_gdc_id,
               a1.platform,
               a1.file_name
        FROM a1 JOIN `{case_table}` AS c ON a1.case_barcode = c.case_barcode and a1.project_short_name = c.project_id
        '''

    return query_bq(sql, step_3_table)


def glue_metadata(step_3_table, raw_rna_seq, step_4_able):
    sql = f"""
        SELECT a.project_short_name,
               a.case_barcode,
               a.primary_site,
               a.sample_type_name,
               a.sample_barcode,
               a.aliquot_barcode,
               REGEXP_EXTRACT(b.gene_id, r"^[^.]+") as Ensembl_gene_id,
               b.gene_id as Ensembl_gene_id_v,
               b.gene_name,
               b.gene_type,
               b.unstranded,
               b.stranded_first,
               b.stranded_second,
               b.tpm_unstranded,
               b.fpkm_unstranded,
               b.fpkm_uq_unstranded,
               a.case_gdc_id,
               a.sample_gdc_id,
               a.aliquot_gdc_id,
               a.file_gdc_id,
               a.platform,
               a.file_name
        FROM `{step_3_table}` AS a JOIN `{raw_rna_seq}` AS b ON a.file_gdc_id = LEFT(b.file_name, 36) 
        WHERE gene_id <> '__no_feature'
            AND gene_id <> '__ambiguous' 
            AND gene_id <> '__too_low_aQual' 
            AND gene_id <> '__not_aligned' 
            AND gene_id <> '__alignment_not_unique' 
        """

    return query_bq(sql, step_4_able)


def extract_platform_for_files(step_1_table, file_table, step_2_table):
    sql = f'''
        WITH
            a1 AS (SELECT DISTINCT analysis_input_file_gdc_ids
                   FROM `{step_1_table}`),
            a2 AS (SELECT a1.analysis_input_file_gdc_ids,
                          b.platform
                   FROM a1 JOIN `{file_table}` as b ON a1.analysis_input_file_gdc_ids = b.file_gdc_id)
        SELECT
               b.project_short_name,
               b.analysis_input_file_gdc_ids,
               b.case_gdc_id,
               b.aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               a2.platform
        FROM `{step_1_table}` AS b 
        JOIN a2 ON a2.analysis_input_file_gdc_ids = b.analysis_input_file_gdc_ids 
        '''

    return query_bq(sql, step_2_table)


def merge_samples_by_aliquot(input_table, output_table):
    sql = f"""
    SELECT 
        project_short_name,
        primary_site,
        case_barcode, 
        string_agg(sample_barcode, ';') as sample_barcode,
        aliquot_barcode,
        gene_name,
        gene_type,
        Ensembl_gene_id,
        Ensembl_gene_id_v,
        unstranded,
        stranded_first,
        stranded_second,
        tpm_unstranded,
        fpkm_unstranded,
        fpkm_uq_unstranded,
        sample_type_name,
        case_gdc_id,
        string_agg(sample_gdc_id, ';') as sample_gdc_id,
        aliquot_gdc_id,
        file_gdc_id,
        platform
    FROM 
        `{input_table}`
    group by 
        project_short_name,
        primary_site,
        case_barcode, 
        aliquot_barcode,
        gene_name,
        gene_type,
        Ensembl_gene_id,
        Ensembl_gene_id_v,
        unstranded,
        stranded_first,
        stranded_second,
        tpm_unstranded,
        fpkm_unstranded,
        fpkm_uq_unstranded,
        sample_type_name,
        case_gdc_id,
        aliquot_gdc_id,
        file_gdc_id,
        platform
    """

    return query_bq(sql, output_table)


def create_rna_seq_table(raw_rna_seq, draft_rna_seq, file_table, aliquot_table, case_table, project_id, dataset, release):
    rna_seq_logger.info("Creating {draft_rna_seq}")

    created_tables = []
    step_1_table = f"{raw_rna_seq}_step_1"
    step_2_table = f"{raw_rna_seq}_step_2"
    step_3_table = f"{raw_rna_seq}_step_3"
    step_4_table = f"{raw_rna_seq}_step_4"
    step_5_table = f"{raw_rna_seq}_step_5"


    # todo describe
    gather_aliquot_ids_results = gather_aliquot_ids(f"{project_id}.{dataset}.{raw_rna_seq}",
                                                    f"{file_table}_{release}",
                                                    f"{project_id}.{dataset}.{step_1_table}")
    if gather_aliquot_ids_results == 'DONE':
        created_tables.append(step_1_table)
    else:
        rna_seq_logger.error("Creating RNA Seq aliquot id table failed")
        sys.exit()

    # todo

    extract_platform_for_files_results = extract_platform_for_files(f"{project_id}.{dataset}.{step_1_table}",
                                                                    f"{file_table}_{release}",
                                                                    f"{project_id}.{dataset}.{step_2_table}")
    if extract_platform_for_files_results == 'DONE':
        created_tables.append(step_2_table)
    else:
        rna_seq_logger.error("Creating platform table failed")
        sys.exit()

    # todo describe
    add_barcodes_to_aliquot_results = add_barcodes_to_aliquot(f"{project_id}.{dataset}.{step_2_table}",
                                                              f"{aliquot_table}_{release}",
                                                              f"{case_table}_{release}",
                                                              f"{project_id}.{dataset}.{step_3_table}")

    if add_barcodes_to_aliquot_results == 'DONE':
        created_tables.append(step_3_table)
    else:
        rna_seq_logger.error("Creating RNA seq barcodes table failed")
        sys.exit()

    # todo describe
    glue_metadata_results = glue_metadata(f"{project_id}.{dataset}.{step_3_table}",
                                          f"{project_id}.{dataset}.{raw_rna_seq}",
                                          f"{project_id}.{dataset}.{step_4_table}")
    if glue_metadata_results == 'DONE':
        created_tables.append(step_4_table)
    else:
        rna_seq_logger.error("Creating add metadata table failed")
        sys.exit()

    # todo describe
    merge_samples_by_aliquot_results = merge_samples_by_aliquot(f"{project_id}.{dataset}.{step_4_table}",
                                                                f"{project_id}.{dataset}.{step_5_table}")
    if merge_samples_by_aliquot_results == 'DONE':
        created_tables.append(step_5_table)
    else:
        rna_seq_logger.error("Creating merge table failed")
        sys.exit()

    # todo describe
    cluster_fields = ["project_short_name", "case_barcode", "sample_barcode", "aliquot_barcode"]
    cluster_table_result = cluster_table(f"{project_id}.{dataset}.{step_5_table}",
                                         f"{project_id}.{dataset}.{draft_rna_seq}", cluster_fields)
    if cluster_table_result.total_rows < 1:
        created_tables.append(draft_rna_seq)
    else:
        print(cluster_table_result)
        rna_seq_logger.error("Creating RNA draft table failed")
        sys.exit()

    return created_tables

