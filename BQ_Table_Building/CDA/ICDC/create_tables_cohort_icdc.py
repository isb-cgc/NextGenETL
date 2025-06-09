import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_table_sql() -> str:
    return f"""    
    SELECT * 
    FROM `{create_dev_table_id(PARAMS, 'cohort')}isb-project-zero.cda_icdc_raw.2025_03_cohort`
    LEFT JOIN `{create_dev_table_id(PARAMS, 'study_arm_cohort_id')}`
      USING (cohort_id)
    LEFT JOIN `{create_dev_table_id(PARAMS, 'study_arm')}` 
      USING (arm_id)
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
