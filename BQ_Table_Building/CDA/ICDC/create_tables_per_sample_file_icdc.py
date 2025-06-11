import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import (load_config, create_dev_table_id, format_seconds, create_per_sample_table_id)
from cda_bq_etl.bq_helpers import (create_table_from_query, update_table_schema_from_generic, query_and_retrieve_result,
                                   get_program_schema_tags_icdc)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_program_acronym_sql() -> str:
    return f"""
        SELECT DISTINCT program_acronym
        FROM `{create_dev_table_id(PARAMS, 'program')}`
    """


def make_row_count_sql(program) -> str:
    return f"""    
        SELECT COUNT(*) AS count
        FROM `{create_dev_table_id(PARAMS, 'sample')}` s
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_file_uuid')}` sf
          USING (sample_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'file')}` f
          ON sf.file_uuid = f.uuid
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sc 
          USING (sample_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_clinical_study_designation')}` ccsd
            USING (case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'program_clinical_study_designation')}` pcsd
            USING (clinical_study_designation)
        WHERE pcsd.program_acronym = '{program}'
    """


def make_table_sql(program) -> str:
    return f"""    
        SELECT sf.file_uuid, sc.case_id, pcsd.program_acronym, s.*, f.*
        FROM `{create_dev_table_id(PARAMS, 'sample')}` s
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_file_uuid')}` sf
          USING (sample_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'file')}` f
          ON sf.file_uuid = f.uuid
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sc 
          USING (sample_id)
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

            count_result = query_and_retrieve_result(make_row_count_sql(program))

            for row in count_result:
                count = row['count']
                break

            if count == 0:
                logger.info(f"No records found for program {program}. Table will not be created.")
                continue

            logger.info(f"Creating table for {program}!")

            create_table_from_query(params=PARAMS,
                                    table_id=create_per_sample_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                    query=make_table_sql(program))

            schema_tags = get_program_schema_tags_icdc(program)

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=create_per_sample_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                             schema_tags=schema_tags)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
