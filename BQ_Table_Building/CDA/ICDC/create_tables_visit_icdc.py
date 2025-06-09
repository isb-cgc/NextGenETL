import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id
from cda_bq_etl.bq_helpers import (create_table_from_query, update_table_schema_from_generic, query_and_retrieve_result)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


# todo should this be a single or program-level table?
def make_visit_sql() -> str:
    return f"""
        SELECT * 
        FROM `{create_dev_table_id(PARAMS, 'visit')}`
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_visit_id')}`
          USING(visit_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'cycle_case_id_and_visit_id')}`
          USING(visit_id)
    """


def make_disease_extent_sql() -> str:
    return f"""
        SELECT * FROM `{create_dev_table_id(PARAMS, 'disease_extent')}`
    """


def make_physical_exam_sql() -> str:
    return f"""
        SELECT * FROM `{create_dev_table_id(PARAMS, 'physical_exam')}`
    """


def make_vital_signs_sql() -> str:
    return f"""
        SELECT * FROM `{create_dev_table_id(PARAMS, 'vital_signs')}`
    """


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

    if 'retrieve_visit_data_and_build_jsonl' in steps:
        visit_result = query_and_retrieve_result(make_visit_sql())

        visit_result_dict = dict(visit_result)

        print(visit_result_dict)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
