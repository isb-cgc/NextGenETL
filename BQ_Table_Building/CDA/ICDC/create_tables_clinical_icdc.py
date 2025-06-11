import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id, \
    create_clinical_table_id
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic, \
    get_program_schema_tags_icdc, query_and_retrieve_result

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_program_acronym_sql() -> str:
    return f"""
        SELECT DISTINCT program_acronym
        FROM `{create_dev_table_id(PARAMS, 'program')}`
    """


# todo these tables all merge because there's only one record, but actual script should account for
#  the possibility of multiple diagnoses
def make_table_sql(program) -> str:
    return f"""
        SELECT * 
        FROM `{create_dev_table_id(PARAMS, 'case')}`
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_clinical_study_designation')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_cohort_id')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'cohort')}`
            USING(cohort_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'demographic')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_diagnosis_id')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'diagnosis')}`
            USING(diagnosis_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_enrollment_id')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'enrollment')}`
            USING(enrollment_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'canine_individual')}`
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'prior_surgery')}`
            USING(enrollment_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_clinical_study_designation')}` ccsd
            USING (case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'program_clinical_study_designation')}` pcsd
            USING (clinical_study_designation)
        WHERE pcsd.program_acronym = '{program}'
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

    if 'create_table_from_query' in steps:
        logger.info("Entering create_table_from_query")

        program_result = query_and_retrieve_result(make_program_acronym_sql())

        for row in program_result:
            program = row['program_acronym']

            logger.info(f"Creating table for {program}!")

            create_table_from_query(params=PARAMS,
                                    table_id=create_clinical_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                    query=make_table_sql(program))

            schema_tags = get_program_schema_tags_icdc(program)

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=create_clinical_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                             schema_tags=schema_tags)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
