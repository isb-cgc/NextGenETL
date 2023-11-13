"""
Copyright 2020-2021, Institute for Systems Biology
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

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                                   create_and_load_table_from_tsv, query_and_retrieve_result, list_tables_in_dataset)
from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.data_helpers import initialize_logging, make_string_bq_friendly, write_list_to_tsv, \
    create_normalized_tsv
from cda_bq_etl.utils import format_seconds, get_filepath, load_config, get_scratch_fp, calculate_md5sum, \
    create_dev_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'programs', 'steps')


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, programs, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    logger.info(f"GDC clinical file script started at {time.strftime('%x %X', time.localtime())}")

    if 'analyze_tables' in steps:
        prefix = f"{PARAMS['RELEASE']}_TARGET"

        table_list = list_tables_in_dataset(project_dataset_id="isb-project-zero.clinical_from_files_raw",
                                            filter_terms=prefix)

        project_tables = dict()

        for table in table_list:
            if "_CDE_" in table:
                continue

            project = table.split("_")[2]

            if project not in project_tables:
                project_tables[project] = list()

            project_tables[project].append(table)

        column_dict = dict()

        for project, table_list in project_tables.items():
            print(project)

            for table in table_list:
                table_id = f"isb-project-zero.clinical_from_files_raw.{table}"

                table_name = table_id.split(".")[-1]
                dataset_id = ".".join(table_id.split(".")[0:-1])

                if 'Supplement' in table_name or 'CDE' in table_name:
                    continue

                column_sql = f"""
                    SELECT column_name
                    FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '{table_name}' 
                """

                res = query_and_retrieve_result(column_sql)

                for row in res:
                    if row[0] not in column_dict:
                        column_dict[row[0]] = 1
                    else:
                        column_dict[row[0]] += 1

        for column, count in sorted(column_dict.items(), key=lambda x: x[1], reverse=True):
            print(f"{column}\t{count}")

        """
        TODO:
        Create merged table.
        Merge in aliquot fields.
        Update field/table metadata.
        Publish.
        Delete working tables.
        """
    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == '__main__':
    main(sys.argv)
