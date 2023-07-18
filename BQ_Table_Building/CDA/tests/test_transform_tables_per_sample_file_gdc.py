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

from google.cloud import bigquery

from BQ_Table_Building.CDA.tests.shared_functions import compare_row_counts, compare_id_keys, compare_table_columns
from common_etl.cda_utils import create_program_name_set
from common_etl.utils import load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    client = bigquery.Client()

    table_id_tuple_set = set()

    program_set = create_program_name_set(API_PARAMS, BQ_PARAMS)

    for program in sorted(program_set):
        if program == "BEATAML1.0":
            program_name = "BEATAML1_0"
        elif program == "EXCEPTIONAL_RESPONDERS":
            program_name = "EXC_RESPONDERS"
        else:
            program_name = program

        gdc_table_name = f"{program_name}_per_sample_file_metadata_hg38_gdc_{API_PARAMS['GDC_RELEASE']}"
        gdc_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['GDC_WORKING_DATASET']}.{gdc_table_name}"
        cda_table_name = f"per_sample_file_metadata_hg38_{program_name}_{API_PARAMS['RELEASE']}"
        cda_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{cda_table_name}"

        # check for valid hg38 table location
        gdc_table = client.get_table(table=gdc_table_id)
        cda_table = client.get_table(table=cda_table_id)

        if gdc_table is None or cda_table is None:
            if gdc_table is None:
                print(f"No table found: {gdc_table_id}")
            if cda_table is None:
                print(f"No table found: {cda_table_id}")
        else:
            table_id_tuple = (program, gdc_table_id, cda_table_id)
            table_id_tuple_set.add(table_id_tuple)

    for table_id_tuple in table_id_tuple_set:
        program = table_id_tuple[0]
        gdc_table_id = table_id_tuple[1]
        cda_table_id = table_id_tuple[2]

        print("\n\n**********")
        print(f"For program: {program}")
        print("**********\n")

        print("Comparing row counts!\n")

        compare_row_counts(old_table_id=gdc_table_id,
                           new_table_id=cda_table_id)

        print("Comparing primary table keys!\n")

        compare_id_keys(old_table_id=gdc_table_id,
                        new_table_id=cda_table_id,
                        primary_key=BQ_PARAMS['PRIMARY_KEY'])

        print("Comparing secondary table keys!\n")

        compare_id_keys(old_table_id=gdc_table_id,
                        new_table_id=cda_table_id,
                        primary_key=BQ_PARAMS['SECONDARY_KEY'])

        columns_list = BQ_PARAMS["COLUMNS"]

        print("Comparing table columns!\n")

        compare_table_columns(old_table_id=gdc_table_id,
                              new_table_id=cda_table_id,
                              primary_key=BQ_PARAMS['PRIMARY_KEY'],
                              secondary_key=BQ_PARAMS['SECONDARY_KEY'],
                              columns=columns_list)


if __name__ == "__main__":
    main(sys.argv)
