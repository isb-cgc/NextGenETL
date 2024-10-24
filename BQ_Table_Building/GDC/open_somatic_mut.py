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

som_mut_logger = logging.getLogger(name='base_script.som_mut')


def collect_barcodes(maf_table, aliquot_table, case_table, dest_table):
    """
    Gather and add sample information to the draft table from the aliquot table
    :param maf_table: draft table to add the aliquot information to
    :type maf_table: basestring
    :param aliquot_table: metadata table id for the aliquot information
    :type aliquot_table: basestring
    :param dest_table: table id for the new table
    :type dest_table: basestring
    :param case_table: metadata table id for the case information
    :type case_table: basestring
    :return: if the SQL query worked
    :rtype: bool
    """

    sql = f"""
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

    return query_bq(sql, dest_table)


def barcode_raw_table_merge(maf_table, barcode_table, dest_table):
    """
    Glue the New Info to the Raw Data Table.
    :param maf_table: table id for the draft maf table
    :type maf_table: basestring
    :param barcode_table: table id for the barcode table
    :type barcode_table: basestring
    :param dest_table: table id for the new table
    :type dest_table: basestring
    :return: if the SQL query worked
    :rtype: bool
    """

    sql = f"""
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

    return query_bq(sql, dest_table)


def merge_samples_by_aliquot(input_table, output_table):
    """
    Some samples are pooled and their lines need to be merged by aliquot
    :param input_table: table to merge samples
    :type input_table: basestring
    :param output_table: name of new table
    :type output_table: basestring
    :return: if the SQL query worked
    :rtype: bool
    """

    sql = f"""
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
            LEFT(file_name, 36) as file_gdc_id,
                CASE
                    WHEN callers LIKE '%muse*%' THEN 'Yes*'
                    WHEN callers LIKE '%muse%' THEN 'Yes'
                ELSE
                    'No'
            END
              muse,
              CASE
                WHEN callers LIKE '%mutect2*%' THEN 'Yes*'
                WHEN callers LIKE '%mutect2%' THEN 'Yes'
              ELSE
              'No'
            END
              mutect2,
              CASE
                WHEN callers LIKE '%pindel*%' THEN 'Yes*'
                WHEN callers LIKE '%pindel%' THEN 'Yes'
              ELSE
              'No'
            END
              pindel,
              CASE
                WHEN callers LIKE '%varscan2*%' THEN 'Yes*'
                WHEN callers LIKE '%varscan2%' THEN 'Yes'
              ELSE
              'No'
            END
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
            varscan2,
            aliquot_barcode_tumor, 
            aliquot_barcode_normal"""

    return query_bq(sql, output_table)


def create_somatic_mut_table(raw_somatic_mut, draft_somatic_mut, aliquot_table, case_table, project_id, dataset,
                             release):
    """
    Run through the SQL queries to create the final draft table.
    :param raw_somatic_mut: Initial Somatic Mutation table name
    :param draft_somatic_mut: Draft Somatic Mutation table name
    :param aliquot_table: Metadata table with aliquot data
    :param case_table: Metadata table with case data
    :param project_id: Project of where the tables are to be created
    :param dataset: Dataset of where the tables are to be created
    :param release: GDC release
    :return: list of tables created
    """
    som_mut_logger.info(f"Creating {draft_somatic_mut}")

    created_tables = []
    step_1_table = f"{raw_somatic_mut}_step_1"
    step_2_table = f"{raw_somatic_mut}_step_2"
    step_3_table = f"{raw_somatic_mut}_step_3"

    collect_barcodes_result = collect_barcodes(f"{project_id}.{dataset}.{raw_somatic_mut}",
                                               f"{aliquot_table}_{release}",
                                               f"{case_table}_{release}",
                                               f"{project_id}.{dataset}.{step_1_table}")
    if collect_barcodes_result == 'DONE':
        created_tables.append(step_1_table)
    else:
        som_mut_logger.error("Creating MAF barcodes table failed")
        sys.exit()

    barcode_raw_table_merge_result = barcode_raw_table_merge(f"{project_id}.{dataset}.{raw_somatic_mut}",
                                                             f"{project_id}.{dataset}.{step_1_table}",
                                                             f"{project_id}.{dataset}.{step_2_table}")
    if barcode_raw_table_merge_result == 'DONE':
        created_tables.append(step_2_table)
    else:
        som_mut_logger.error("Creating MAF intermediate table failed")
        sys.exit()

    merge_samples_by_aliquot_result = merge_samples_by_aliquot(f"{project_id}.{dataset}.{step_2_table}",
                                                               f"{project_id}.{dataset}.{step_3_table}")
    if merge_samples_by_aliquot_result == 'DONE':
        created_tables.append(step_3_table)
    else:
        som_mut_logger.error("Creating MAF merge table failed")
        sys.exit()

    cluster_fields = ["project_short_name", "case_barcode", "sample_barcode_tumor", "aliquot_barcode_tumor"]
    cluster_table_result = cluster_table(f"{project_id}.{dataset}.{step_3_table}",
                                         f"{project_id}.{dataset}.{draft_somatic_mut}", cluster_fields)
    if cluster_table_result.total_rows < 1:
        created_tables.append(draft_somatic_mut)
    else:
        som_mut_logger.error("Creating MAF draft table failed")
        sys.exit()

    return created_tables

