import sys
import time
import logging
from typing import Any

from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload
from cda_bq_etl.utils import (load_config, create_dev_table_id, format_seconds, create_clinical_table_id)
from cda_bq_etl.bq_helpers import (update_table_schema_from_generic, query_and_retrieve_result,
                                   create_and_upload_schema_for_json, retrieve_bq_schema_object,
                                   create_and_load_table_from_jsonl, get_program_schema_tags_icdc)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_program_acronym_sql() -> str:
    return f"""
        SELECT DISTINCT program_acronym
        FROM `{create_dev_table_id(PARAMS, 'program')}`
    """


# I also recycle this query to filter vital_signs, disease_extent and physical_exam by program.
# BQ caches results for 24 hours to reduce costs, so reusing it isn't computationally expensive.
def make_visit_sql(program) -> str:
    return f"""
        WITH merged_case_ids AS (
            SELECT v.visit_id, 
                v.visit_date,
                COALESCE(cv.case_id, ccv.case_id) AS case_id
            FROM `{create_dev_table_id(PARAMS, 'visit')}` v 
            LEFT JOIN `{create_dev_table_id(PARAMS, 'case_visit_id')}` cv
                USING(visit_id)
            LEFT JOIN `{create_dev_table_id(PARAMS, 'cycle_case_id_and_visit_id')}` ccv
                USING(visit_id)
        )
        SELECT visit_id, case_id, visit_date, program_acronym
        FROM merged_case_ids
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_clinical_study_designation')}` ccsd
            USING(case_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'program_clinical_study_designation')}` pcsd
            USING (clinical_study_designation)
        WHERE pcsd.program_acronym = '{program}'
    """


def make_child_table_sql(program, table_type) -> str:
    logger = logging.getLogger('base_script')

    if table_type not in PARAMS['TABLE_COLUMNS']:
        logger.critical(f"Table type {table_type} not defined in yaml config. Please add it and re-run the script.")
        exit(-1)

    column_list = PARAMS['TABLE_COLUMNS'][table_type]

    select_statement = "SELECT "

    # either select all columns from child table, or select based on list from TABLE_COLUMNS in yaml config.
    if not column_list:
        select_statement += "child_table.* "
    else:
        for column in column_list:
            select_statement += f"child_table.{column}, "
        select_statement = select_statement[:-2] + " "

    return f"""
        WITH visit_program_mapping AS ({make_visit_sql(program)})
        {select_statement}
        FROM `{create_dev_table_id(PARAMS, table_type)}` child_table
        LEFT JOIN visit_program_mapping
            USING (visit_id)
        WHERE program_acronym = '{program}'
    """


def create_child_field_dict(visit_case_mapping: dict[str, str],
                            program: str,
                            table_type: str,
                            exclude_columns: list[str] = None) -> dict[str, list] | None:
    logger = logging.getLogger('base_script')

    child_table_result = query_and_retrieve_result(make_child_table_sql(program, table_type))

    if child_table_result.total_rows == 0:
        logger.info(f"No rows found for {table_type} in {program}, skipping.")
        return None
    else:
        logger.info(f"Appending {table_type} to {program} visit table.")

    visit_child_row_dict = dict()

    for row in child_table_result:
        visit_id = row['visit_id']

        # confirm visit_id is non-null and that it can be mapped to a case_id
        if not visit_id:
            logger.warning(f"No visit id {visit_id} found in {table_type}. Skipping row; investigate.")
            continue
        elif visit_id not in visit_case_mapping:
            logger.warning(f"visit id {visit_id} found in {table_type} but not in visit_case_mapping. "
                           f"Skipping row; investigate.")
            continue

        child_row_dict = dict()

        for column in PARAMS['TABLE_COLUMNS'][table_type]:
            if column not in exclude_columns:
                child_row_dict[column] = row[column]

        if visit_id not in visit_child_row_dict:
            visit_child_row_dict[visit_id] = list()

        visit_child_row_dict[visit_id].append(child_row_dict)

    return visit_child_row_dict


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

    program_result = query_and_retrieve_result(make_program_acronym_sql())

    for row in program_result:
        program = row['program_acronym']

        if 'retrieve_visit_data_and_build_jsonl' in steps:
            logger.info("Entering retrieve_visit_data_and_build_jsonl")

            visit_result = query_and_retrieve_result(make_visit_sql(program))

            if visit_result.total_rows == 0:
                logger.info(f"No visit data found for {program}. No table will be created.")
                continue
            else:
                logger.info(f"Creating table for {program}!")

            logger.info("Creating visit dict")

            cases_visits_dict = dict()
            visits_dict = dict()

            # map visit ids to case ids, allowing for identification of the correct location to insert child data
            visit_case_mapping = dict()

            for visit_row in visit_result:
                visit_id = visit_row['visit_id']
                visit_date = visit_row['visit_date']
                case_id = visit_row['case_id']

                if not case_id:
                    logger.error(f"No case_id match for visit_id: {visit_id}. Investigate. Skipping record.")
                    continue

                # Create the initial parent entry if this case doesn't already exist in the dict.
                if case_id not in cases_visits_dict:
                    cases_visits_dict[case_id] = {
                        'case_id': case_id,
                        'visits': list()
                    }

                if visit_id in visits_dict:
                    logger.warning(f"visit_id {visit_id} already exists in visits dict. Investigate. Skipping record.")

                # Add the visit data and initialize the nested lists for child field groups.
                visits_dict[visit_id] = {
                    'visit_id': visit_id,
                    'visit_date': visit_date
                }

                for table_type in PARAMS['CHILD_TABLE_TYPES']:
                    visits_dict[visit_id][table_type] = list()

                # associate cases and visits so we can aggregate later
                visit_case_mapping[visit_id] = case_id

            # dict looks like this: { table_type: { visit_id: [record dicts] }}
            child_data_dict = dict()

            for table_type in PARAMS['CHILD_TABLE_TYPES']:
                logger.info(f"Appending {table_type}")
                child_data_dict[table_type] = create_child_field_dict(visit_case_mapping=visit_case_mapping,
                                                                      program=program,
                                                                      table_type=table_type,
                                                                      exclude_columns=['visit_id'])

            # retrieve child data for each visit id and append to visits_dict
            for visit_id in visits_dict.keys():
                # append nested visit records to cases_visits_dict
                for table_type in PARAMS['CHILD_TABLE_TYPES']:
                    if visit_id in child_data_dict[table_type]:
                        visits_dict[visit_id][table_type] = child_data_dict[table_type][visit_id]

                case_id = visit_case_mapping[visit_id]
                cases_visits_dict[case_id]['visits'].append(visits_dict[visit_id])

            case_visit_record_list = list()

            # make case record list, convert to jsonl and create the schema
            for case_visit_record in cases_visits_dict.values():
                case_visit_record_list.append(case_visit_record)

            file_prefix = f"{program}_{PARAMS['TABLE_NAME']}"

            write_list_to_jsonl_and_upload(PARAMS, prefix=file_prefix, record_list=case_visit_record_list)

            create_and_upload_schema_for_json(PARAMS,
                                              record_list=case_visit_record_list,
                                              table_name=file_prefix,
                                              include_release=True,
                                              reorder_nesting=True)

        if 'create_table' in steps:
            file_prefix = f"{program}_{PARAMS['TABLE_NAME']}"

            # Download schema file from Google Cloud bucket
            table_schema = retrieve_bq_schema_object(PARAMS, table_name=file_prefix, include_release=True)

            # if there's no schema, this
            if not table_schema:
                logger.info(f"No table schema found for {program}, skipping table creation.")
                continue

            visit_table_id = create_clinical_table_id(PARAMS, table_name=file_prefix)

            # Load jsonl data into BigQuery table
            create_and_load_table_from_jsonl(PARAMS,
                                             jsonl_file=f"{file_prefix}_{PARAMS['RELEASE']}.jsonl",
                                             table_id=visit_table_id,
                                             schema=table_schema)

            schema_tags = get_program_schema_tags_icdc(program)

            update_table_schema_from_generic(params=PARAMS, table_id=visit_table_id, schema_tags=schema_tags)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
