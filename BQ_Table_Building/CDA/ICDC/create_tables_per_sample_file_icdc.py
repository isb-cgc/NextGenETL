import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


# todo make program-level tables
def make_table_sql() -> str:
    return f"""    
        SELECT sf.file_uuid, sc.case_id, s.*, f.*
        FROM `{create_dev_table_id(PARAMS, 'sample')}` s
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_file_uuid')}` sf
          USING (sample_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'file')}` f
          ON sf.file_uuid = f.uuid
        LEFT JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sc 
          USING (sample_id)
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

        # todo change to per_sample_file table id
        create_table_from_query(params=PARAMS,
                                table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                query=make_table_sql())

        # todo change to per_sample_file table id
        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
