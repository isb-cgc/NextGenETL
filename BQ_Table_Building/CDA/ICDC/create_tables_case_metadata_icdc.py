import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id
from cda_bq_etl.bq_helpers.create_modify import create_table_from_query, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_table_sql() -> str:
    return f"""    
        WITH file_counts AS (
          SELECT case_id, count(file_uuid) AS file_count
          FROM `isb-project-zero.cda_icdc_raw.2025_03_case_file_uuid`
          GROUP BY case_id
        )
        
        SELECT c.case_id, pcsd.program_acronym, p.program_name, csd.clinical_study_designation, s.clinical_study_id, 
            s.clinical_study_name, s.accession_id, COALESCE(fc.file_count, 0) AS file_count
        FROM `{create_dev_table_id(PARAMS, 'case')}` c
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_clinical_study_designation')}` csd
          USING (case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'program_clinical_study_designation')}` pcsd
          USING (clinical_study_designation)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'program')}` p
          USING (program_acronym)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'study')}` s
          USING (clinical_study_designation)
        LEFT JOIN file_counts fc
          USING (case_id)
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

        create_table_from_query(params=PARAMS,
                                table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                query=make_table_sql())

        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
