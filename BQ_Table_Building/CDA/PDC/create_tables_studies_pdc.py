"""
Copyright 2023, Institute for Systems Biology

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
import time

from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, delete_bq_table

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    delete_tables = [
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_BEATAML1_0_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_CDDP_EAGLE_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_CGCI_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_CMI_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_CPTAC_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_CTSP_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_EXC_RESPONDERS_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_FM_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_GENIE_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_HCMI_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_MATCH_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_MMRF_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_MP2PRT_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_NCICCR_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_OHSU_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_ORGANOID_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_REBC_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_TARGET_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_TCGA_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_TRIO_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_VAREPOP_2023_03",
        "isb-project-zero.cda_gdc_per_sample_file.per_sample_file_metadata_hg38_WCDT_2023_03"
    ]

    for table in delete_tables:
        delete_bq_table(table)

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
