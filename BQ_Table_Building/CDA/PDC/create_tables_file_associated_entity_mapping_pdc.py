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
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_associated_entity_query() -> str:
    """
    Make BigQuery sql statement, used to generate the file_associated_entity_mapping table.
    :return: sql query statement
    """
    return f"""
        SELECT fa.file_id,
            ac.case_id,
            a.aliquot_id,
            a.aliquot_submitter_id,
            "aliquot" AS entity_type
        FROM `isb-project-zero.cda_pdc_raw.2023_06_file_aliquot_id` fa
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_aliquot` a
            ON a.aliquot_id = fa.aliquot_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_aliquot_case_id` ac
            ON ac.aliquot_id = a.aliquot_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    # code here

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
