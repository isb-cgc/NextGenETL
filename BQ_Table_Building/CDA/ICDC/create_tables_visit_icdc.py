import sys
import time

from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id, \
    create_clinical_table_id
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


def make_vital_signs_sql(program) -> str:
    return f"""
        WITH visit_program_mapping AS ({make_visit_sql(program)})
        SELECT vs.* , vp.program_acronym, vp.case_id, vp.visit_id
        FROM `{create_dev_table_id(PARAMS, 'vital_signs')}` vs
        LEFT JOIN visit_program_mapping vp
            USING (visit_id)
        WHERE program_acronym = '{program}'
    """


def make_disease_extent_sql(program) -> str:
    # todo handle the null field exclusions differently
    return f"""
        WITH visit_program_mapping AS ({make_visit_sql(program)})
        SELECT de.* EXCEPT(longest_measurement_unit, longest_measurement_original, longest_measurement_original_unit, 
        previously_treated, previously_irradiated)
        FROM `{create_dev_table_id(PARAMS, 'disease_extent')}` de
        LEFT JOIN visit_program_mapping
            USING (visit_id)
        WHERE program_acronym = '{program}'
    """


def make_physical_exam_sql(program) -> str:
    # todo handle the null field exclusions differently
    return f"""
        WITH visit_program_mapping AS ({make_visit_sql(program)})
        SELECT pe.* EXCEPT(enrollment_id, day_in_cycle, phase_pe, assessment_timepoint)
        FROM `{create_dev_table_id(PARAMS, 'physical_exam')}` pe
        LEFT JOIN visit_program_mapping
            USING (visit_id)
        WHERE program_acronym = '{program}'
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
        logger.info("Entering retrieve_visit_data_and_build_jsonl")
        program_result = query_and_retrieve_result(make_program_acronym_sql())

        for row in program_result:
            program = row['program_acronym']

            logger.info(f"Creating table for {program}!")

            visit_result = query_and_retrieve_result(make_visit_sql(program))

            if visit_result.total_rows == 0:
                logger.info(f"No visit data found for {program}. No table will be created.")
                continue

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
                    'visit_date': visit_date,
                    'vital_signs': list(),
                    'disease_extent': list(),
                    'physical_exam': list()
                }

                visit_case_mapping[visit_id] = case_id

            vital_signs_result = query_and_retrieve_result(make_vital_signs_sql(program))

            logger.info("Appending vital signs")
            for vital_sign_row in vital_signs_result:
                print(vital_sign_row)
                # confirm visit_id field exists in the query results
                if 'visit_id' not in vital_sign_row:
                    logger.warning("No visit_id found in vital_signs result. Should be investigated. Skipping row.")
                    # todo remove
                    exit(-1)
                    continue

                visit_id = vital_sign_row['visit_id']

                # confirm visit_id is non-null and that it can be mapped to a case_id
                if not visit_id:
                    logger.warning(f"No visit id {visit_id} found in vital_signs. Should be investigated. "
                                   f"Skipping row.")
                    continue
                elif visit_id not in visit_case_mapping:
                    logger.warning(f"visit id {visit_id} found in vital_signs but not in visit_case_mapping. "
                                   "Should be investigated. Skipping this row.")
                    continue

                visits_dict[visit_id]['vital_signs'].append({
                    'date_of_vital_signs': vital_sign_row['date_of_vital_signs'],
                    'body_temperature': vital_sign_row['body_temperature'],
                    'body_temperature_unit': vital_sign_row['body_temperature_unit'],
                    'body_temperature_original': vital_sign_row['body_temperature_original'],
                    'body_temperature_original_unit': vital_sign_row['body_temperature_original_unit'],
                    'pulse': vital_sign_row['pulse'],
                    'pulse_unit': vital_sign_row['pulse_unit'],
                    'pulse_original': vital_sign_row['pulse_original'],
                    'pulse_original_unit': vital_sign_row['pulse_original_unit'],
                    'respiration_rate': vital_sign_row['respiration_rate'],
                    'respiration_rate_unit': vital_sign_row['respiration_rate_unit'],
                    'respiration_rate_original': vital_sign_row['respiration_rate_original'],
                    'respiration_rate_original_unit': vital_sign_row['respiration_rate_original_unit'],
                    'respiration_pattern': vital_sign_row['respiration_pattern'],
                    'systolic_bp': vital_sign_row['systolic_bp'],
                    'systolic_bp_unit': vital_sign_row['systolic_bp_unit'],
                    'systolic_bp_original': vital_sign_row['systolic_bp_original'],
                    'systolic_bp_original_unit': vital_sign_row['systolic_bp_original_unit'],
                    'pulse_ox': vital_sign_row['pulse_ox'],
                    'pulse_ox_unit': vital_sign_row['pulse_ox_unit'],
                    'pulse_ox_original': vital_sign_row['pulse_ox_original'],
                    'pulse_ox_original_unit': vital_sign_row['pulse_ox_original_unit'],
                    'patient_weight': vital_sign_row['patient_weight'],
                    'patient_weight_unit': vital_sign_row['patient_weight_unit'],
                    'patient_weight_original': vital_sign_row['patient_weight_original'],
                    'patient_weight_original_unit': vital_sign_row['patient_weight_original_unit'],
                    'body_surface_area': vital_sign_row['body_surface_area'],
                    'body_surface_area_unit': vital_sign_row['body_surface_area_unit'],
                    'body_surface_area_original': vital_sign_row['body_surface_area_original'],
                    'body_surface_area_original_unit': vital_sign_row['body_surface_area_original_unit'],
                    'modified_ecog': vital_sign_row['modified_ecog']
                })

            disease_extent_result = query_and_retrieve_result(make_disease_extent_sql(program))

            logger.info("Appending disease_extent")

            for disease_extent_row in disease_extent_result:
                # confirm visit_id field exists in the query results
                if 'visit_id' not in disease_extent_row:
                    logger.warning("No visit_id found in disease_extent result. Should be investigated. Skipping row.")
                    continue

                visit_id = disease_extent_row['visit_id']

                # confirm visit_id is non-null and that it can be mapped to a case_id
                if not visit_id:
                    logger.warning(f"No visit id {visit_id} found in disease_extent. Should be investigated. "
                                   f"Skipping this row.")
                    continue
                elif visit_id not in visit_case_mapping:
                    logger.warning(f"visit id {visit_id} found in disease_extent but not in visit_case_mapping. "
                                   "Should be investigated. Skipping this row.")
                    continue

                visits_dict[visit_id]['disease_extent'].append({
                    'lesion_number': disease_extent_row['lesion_number'],
                    'lesion_site': disease_extent_row['lesion_site'],
                    'lesion_description': disease_extent_row['lesion_description'],
                    'measurable_lesion': disease_extent_row['measurable_lesion'],
                    'target_lesion': disease_extent_row['target_lesion'],
                    'date_of_evaluation': disease_extent_row['date_of_evaluation'],
                    'measured_how': disease_extent_row['measured_how'],
                    'longest_measurement': disease_extent_row['longest_measurement'],
                    'evaluation_number': disease_extent_row['evaluation_number'],
                    'evaluation_code': disease_extent_row['evaluation_code']
                })

            physical_exam_result = query_and_retrieve_result(make_physical_exam_sql(program))

            logger.info("Appending physical exam")
            for physical_exam_row in physical_exam_result:
                # confirm visit_id field exists in the query results
                if 'visit_id' not in physical_exam_row:
                    logger.warning("No visit_id found in physical_exam result. Should be investigated. Skipping row.")
                    continue

                visit_id = physical_exam_row['visit_id']

                # confirm visit_id is non-null and that it can be mapped to a case_id
                if not visit_id:
                    logger.warning(f"No visit id {visit_id} found in physical_exam. Should be investigated. "
                                   f"Skipping row.")
                    continue
                elif visit_id not in visit_case_mapping:
                    logger.warning(f"visit id {visit_id} found in physical_exam but not in visit_case_mapping. "
                                   "Should be investigated. Skipping this row.")
                    continue

                visits_dict[visit_id]['physical_exam'].append({
                    'date_of_examination': physical_exam_row['date_of_examination'],
                    'body_system': physical_exam_row['body_system'],
                    'pe_finding': physical_exam_row['pe_finding'],
                    'pe_comment': physical_exam_row['pe_comment']
                })

            for visit_id, visit_dict in visits_dict.items():
                # find the case_id associated with the case,
                # append the visit dict to the list of visits in the cases_visits_dict
                case_id = visit_case_mapping[visit_id]
                cases_visits_dict[case_id]['visits'].append(visit_dict)

            case_visit_record_list = list()

            for case_visit_record in cases_visits_dict.values():
                case_visit_record_list.append(case_visit_record)

            file_prefix = f"{program}_{PARAMS['TABLE_NAME']}"

            write_list_to_jsonl_and_upload(PARAMS, prefix=file_prefix, record_list=case_visit_record_list)

            create_and_upload_schema_for_json(PARAMS,
                                              record_list=case_visit_record_list,
                                              table_name=file_prefix,
                                              include_release=True)

    if 'create_table' in steps:
        program_result = query_and_retrieve_result(make_program_acronym_sql())

        for row in program_result:
            program = row['program_acronym']
            file_prefix = f"{program}_{PARAMS['TABLE_NAME']}"

            # Download schema file from Google Cloud bucket
            table_schema = retrieve_bq_schema_object(PARAMS, table_name=file_prefix, include_release=True)

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
