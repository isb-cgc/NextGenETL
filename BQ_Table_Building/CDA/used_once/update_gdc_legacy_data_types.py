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

from cda_bq_etl.bq_helpers import query_and_retrieve_result


def make_data_type_update_query(old_value, new_value):
    table_id = "isb-project-zero.cda_gdc_legacy.fileData_legacy_r37_copy"

    return f"""
        UPDATE `{table_id}`
        SET data_type = '{new_value}'
        WHERE data_type = '{old_value}'
    """


def main(args):
    data_type_dict = {
        "ABI sequence trace": "ABI Sequence Trace",
        "Aligned reads": "Aligned Reads",
        "Auxiliary test": "Auxiliary Test",
        "Biospecimen data": "Biospecimen Data",
        "Bisulfite sequence alignment": "Bisulfite Sequence Alignment",
        "CGH array QC": "CGH Array QC",
        "Clinical data": "Clinical Data",
        "Copy number estimate": "Copy Number Estimate",
        "Copy number QC metrics": "Copy number QC metrics",
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

    for old_value, new_value in data_type_dict:
        update_query = make_data_type_update_query(old_value, new_value)
        affected_row_count = query_and_retrieve_result(update_query)

        if affected_row_count:
            print(f"Updated {old_value} to {new_value}; {affected_row_count} rows affected.")
        else:
            print(f"Error updating {old_value} to {new_value}; None result returned.")


if __name__ == "__main__":
    main(sys.argv)
