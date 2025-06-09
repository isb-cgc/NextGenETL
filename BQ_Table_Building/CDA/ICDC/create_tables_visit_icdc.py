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
        SELECT v.visit_id, v.visit_date, v.visit_number, 
            cv.case_id, 
            ccv.case_id AS cycle_case_id
        FROM `{create_dev_table_id(PARAMS, 'visit')}` v 
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_visit_id')}` cv
          USING(visit_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'cycle_case_id_and_visit_id')}` ccv
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
        visit_dict = dict()

        visit_result = query_and_retrieve_result(make_visit_sql())

        for row in visit_result:
            visit_id = row['visit_id']
            visit_date = row['visit_date']
            visit_number = row['visit_number']
            case_id = row['case_id']
            cycle_case_id = row['cycle_case_id']

            # for whatever reason, the case_visit_id file only contains 30 associations.
            # The cycle_case_and_visit_id table contains most of the needed case-visit associations.
            # However, they seem to be mutually exclusive--most (all?) entries are only in one of the two files.
            # Therefore, we're running a comparison here to flag any issues and to put the case_id in a single field.
            if case_id and cycle_case_id and case_id != cycle_case_id:
                # todo change to critical
                logger.warning(f"Mismatched case_id, visit_case_id ({case_id}, {cycle_case_id} for visit_id: {visit_id}")
            elif not case_id and not cycle_case_id:
                logger.warning(f"No case_id match for visit_id: {visit_id}")
            elif not case_id:
                case_id = cycle_case_id

            if visit_id in visit_dict:
                logger.critical(f"visit id {visit_id} already found in raw data. This shouldn't happen--exiting.")

            visit_dict[visit_id] = {
                'visit_id': visit_id,
                'visit_date': visit_date,
                'visit_number': visit_number,
                'case_id': case_id
            }

            print(visit_dict[visit_id])

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
