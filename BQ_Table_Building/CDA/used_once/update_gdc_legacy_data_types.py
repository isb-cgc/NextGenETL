"""
Copyright 2024, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import sys
from typing import Union

from google.cloud import bigquery

from cda_bq_etl.bq_helpers import query_and_return_row_count, update_friendly_name, copy_bq_table, \
    change_status_to_archived, delete_bq_table, await_job

Params = dict[str, Union[str, dict, int, bool]]


def make_data_type_update_query(old_value, new_value):
    table_id = "isb-project-zero.cda_gdc_legacy.fileData_legacy_r37_v2"

    return f"""
        UPDATE `{table_id}`
        SET data_type = '{new_value}'
        WHERE data_type = '{old_value}'
    """


def copy_table(params: Params, src_table: str, dest_table: str):
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(params, client, bq_job):
        print(f"Successfully copied {src_table} -> ")
        print(f"\t\t\t{dest_table}")


def update_values():
    data_type_dict = {
        "ABI sequence trace": "ABI Sequence Trace",
        "Aligned reads": "Aligned Reads",
        "Auxiliary test": "Auxiliary Test",
        "Biospecimen data": "Biospecimen Data",
        "Bisulfite sequence alignment": "Bisulfite Sequence Alignment",
        "CGH array QC": "CGH Array QC",
        "Clinical data": "Clinical Data",
        "Copy number estimate": "Copy Number Estimate",
        "Copy number QC metrics": "Copy Number QC Metrics",
        "Copy number segmentation": "Copy Number Segmentation",
        "Copy number variation": "Copy Number Variation",
        "Diagnostic image": "Diagnostic Image",
        "Exon junction quantification": "Exon Junction Quantification",
        "Exon quantification": "Exon Quantification",
        "Gene expression quantification": "Gene Expression Quantification",
        "Gene expression summary": "Gene Expression Summary",
        "Isoform expression quantification": "Isoform Expression Quantification",
        "Methylation array QC metrics": "Methylation Array QC Metrics",
        "Methylation beta value": "Methylation Beta Value",
        "Methylation percentage": "Methylation Percentage",
        "Microsatellite instability": "Microsatellite Instability",
        "miRNA gene quantification": "miRNA Gene Quantification",
        "miRNA isoform quantification": "miRNA Isoform Quantification",
        "Normalized copy numbers": "Normalized Copy Numbers",
        "Normalized intensities": "Normalized Intensities",
        "Pathology report": "Pathology Report",
        "Processed intensities": "Processed Intensities",
        "Protein expression quantification": "Protein Expression Quantification",
        "Raw intensities": "Raw Intensities",
        "rtPCR quantification": "rtPCR Quantification",
        "Sequencing tag": "Sequencing Tag",
        "Sequencing tag counts": "Sequencing Tag Counts",
        "Simple nucleotide variation": "Simple Nucleotide Variation",
        "Simple somatic mutation": "Simple Somatic Mutation",
        "Structural variation": "Structural Variation",
        "Tissue slide image": "Tissue Slide Image",
        "Unaligned reads": "Unaligned Reads"
    }

    for old_value, new_value in data_type_dict.items():
        update_query = make_data_type_update_query(old_value, new_value)
        affected_row_count = query_and_return_row_count(update_query)

        if affected_row_count:
            print(f"Updated {old_value} to {new_value}; {affected_row_count} rows affected.")
        else:
            print(f"Error updating {old_value} to {new_value}; None result returned.")


def publish_table(table_ids: dict[str, str]):
    params = {
        'LOCATION': 'US'
    }

    print(f"Publishing {table_ids['versioned']}")
    copy_table(params, table_ids['source'], dest_table=table_ids['versioned'])

    print(f"Publishing {table_ids['current']}")
    copy_table(params, table_ids['source'], dest_table=table_ids['current'])

    current_friendly_name = 'FILE METADATA GDC LEGACY ARCHIVE'
    print(f"Updating friendly name for {table_ids['current']}")
    update_friendly_name(params, table_id=table_ids['current'], custom_name=current_friendly_name)

    if table_ids['previous_versioned']:
        print(f"Archiving {table_ids['previous_versioned']}")
        change_status_to_archived(table_ids['previous_versioned'])


def main(args):
    table_ids = {
        "source": "isb-project-zero.cda_gdc_legacy.fileData_legacy_r37_v2",
        "versioned": "isb-cgc-bq.GDC_case_file_metadata_versioned.fileData_legacy_r37_v2",
        "current": "isb-cgc-bq.GDC_case_file_metadata.fileData_legacy_current",
        "previous_versioned": "isb-cgc-bq.GDC_case_file_metadata_versioned.fileData_legacy_r37"
    }

    publish_table(table_ids)


if __name__ == "__main__":
    main(sys.argv)
