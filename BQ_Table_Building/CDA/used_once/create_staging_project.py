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

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, format_seconds
from cda_bq_etl.bq_helpers import create_bq_dataset

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def create_datasets():
    for dataset in PARAMS['NEW_DATASETS']:
        create_bq_dataset(PARAMS, project_id=PARAMS['STAGING_PROJECT'], dataset_name=dataset)
        create_bq_dataset(PARAMS, project_id=PARAMS['STAGING_PROJECT'], dataset_name=f"{dataset}_versioned")


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    # create datasets
    if 'create_datasets' in steps:
        create_datasets()
    if 'copy_tables' in steps:
        # find tables in each program dataset that have PARAMS['NODE'] in the name, as well as one of the table keywords
        # if current:
        # - if clinical, alter names using WORDS_TO_ALTER
        # - copy all these tables
        # if versioned:
        # - if clinical, alter names using WORDS_TO_ALTER
        # - only copy the newest version. Find all the table types, sort in desc order, take the first one?
        pass


    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
