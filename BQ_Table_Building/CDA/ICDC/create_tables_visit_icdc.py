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
        SELECT v.visit_id, v.visit_date, 
            cv.case_id, 
            ccv.case_id AS cycle_case_id
        FROM `{create_dev_table_id(PARAMS, 'visit')}` v 
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_visit_id')}` cv
          USING(visit_id)
        LEFT JOIN `{create_dev_table_id(PARAMS, 'cycle_case_id_and_visit_id')}` ccv
          USING(visit_id)
    """


def make_disease_extent_sql() -> str:
    # todo handle the null field exclusions differently
    return f"""
        SELECT * EXCEPT(longest_measurement_unit, longest_measurement_original, longest_measurement_original_unit, 
        previously_treated, previously_irradiated)
        FROM `{create_dev_table_id(PARAMS, 'disease_extent')}`
    """


def make_physical_exam_sql() -> str:
    # todo handle the null field exclusions differently
    return f"""
        SELECT * EXCEPT(enrollment_id, day_in_cycle, phase_pe, assessment_timepoint)
        FROM `{create_dev_table_id(PARAMS, 'physical_exam')}`
    """


def make_vital_signs_sql() -> str:
    # todo handle the null field exclusions differently
    return f"""
        SELECT * 
        FROM `{create_dev_table_id(PARAMS, 'vital_signs')}`
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
            case_id = row['case_id']
            cycle_case_id = row['cycle_case_id']

            # The case_visit_id raw mapping file only contains 30 associations--cycle_case_and_visit_id contains most
            # of the needed case-visit associations. However, they seem to be mutually exclusive--currently, the
            # entries only exist in one of the two files. Therefore, we're running a comparison here to flag any future
            # issues and to put the case_id in a single field.
            if case_id and cycle_case_id and case_id != cycle_case_id:
                logger.critical(f"Mismatched case_id, visit_case_id ({case_id}, {cycle_case_id} "
                                f"for visit_id: {visit_id}. Exiting.")
            elif not case_id and not cycle_case_id:
                logger.warning(f"No case_id match for visit_id: {visit_id}. This warrants further investigation.")
            elif not case_id:
                case_id = cycle_case_id

            if visit_id in visit_dict:
                logger.critical(f"visit id {visit_id} already found in raw data. This shouldn't happen--exiting.")

            visit_dict[visit_id] = {
                'visit_id': visit_id,
                'visit_date': visit_date,
                'case_id': case_id,
                'disease_extent': list(),
                'physical_exam': list(),
                'vital_signs': list()
            }

        disease_extent_result = query_and_retrieve_result(make_disease_extent_sql())

        for row in disease_extent_result:
            visit_id = row['visit_id']

            if not visit_id:
                logger.warning(f"No visit id {visit_id} found in disease_extent. Should be investigated. "
                               f"Skipping this row.")
            elif visit_id not in visit_dict:
                logger.warning(f"visit id {visit_id} found in disease_extent raw data but not in visit. "
                               "Should be investigated. Skipping this row.")

            visit_dict[visit_id]['disease_extent'].append({
                'lesion_number': row['lesion_number'],
                'lesion_site': row['lesion_site'],
                'lesion_description': row['lesion_description'],
                'measurable_lesion': row['measurable_lesion'],
                'target_lesion': row['target_lesion'],
                'date_of_evaluation': row['date_of_evaluation'],
                'measured_how': row['measured_how'],
                'longest_measurement': row['longest_measurement'],
                'evaluation_number': row['evaluation_number'],
                'evaluation_code': row['evaluation_code']
            })

        physical_exam_result = query_and_retrieve_result(make_physical_exam_sql())

        for row in physical_exam_result:
            visit_id = row['visit_id']

            if not visit_id:
                logger.warning(f"No visit id {visit_id} found in physical_exam. Should be investigated. "
                               f"Skipping this row.")
            elif visit_id not in visit_dict:
                logger.warning(f"visit id {visit_id} found in physical_exam raw data but not in visit. "
                               "Should be investigated. Skipping this row.")

            visit_dict[visit_id]['physical_exam'].append({
                'date_of_examination': row['date_of_examination'],
                'body_system': row['body_system'],
                'pe_finding': row['pe_finding'],
                'pe_comment': row['pe_comment']
            })

        vital_signs_result = query_and_retrieve_result(make_vital_signs_sql())

        for row in vital_signs_result:
            visit_id = row['visit_id']

            if not visit_id:
                logger.warning(f"No visit id {visit_id} found in vital_signs. Should be investigated. "
                               f"Skipping this row.")
            elif visit_id not in visit_dict:
                logger.warning(f"visit id {visit_id} found in vital_signs raw data but not in visit. "
                               "Should be investigated. Skipping this row.")

            visit_dict[visit_id]['vital_signs'].append({
                'date_of_vital_signs': row['date_of_vital_signs'],
                'body_temperature': row['body_temperature'],
                'body_temperature_unit': row['body_temperature_unit'],
                'body_temperature_original': row['body_temperature_original'],
                'body_temperature_original_unit': row['body_temperature_original_unit'],
                'pulse': row['pulse'],
                'pulse_unit': row['pulse_unit'],
                'pulse_original': row['pulse_original'],
                'pulse_original_unit': row['pulse_original_unit'],
                'respiration_rate': row['respiration_rate'],
                'respiration_rate_unit': row['respiration_rate_unit'],
                'respiration_rate_original': row['respiration_rate_original'],
                'respiration_rate_original_unit': row['respiration_rate_original_unit'],
                'respiration_pattern': row['respiration_pattern'],
                'systolic_bp': row['systolic_bp'],
                'systolic_bp_unit': row['systolic_bp_unit'],
                'systolic_bp_original': row['systolic_bp_original'],
                'systolic_bp_original_unit': row['systolic_bp_original_unit'],
                'pulse_ox': row['pulse_ox'],
                'pulse_ox_unit': row['pulse_ox_unit'],
                'pulse_ox_original': row['pulse_ox_original'],
                'pulse_ox_original_unit': row['pulse_ox_original_unit'],
                'patient_weight': row['patient_weight'],
                'patient_weight_unit': row['patient_weight_unit'],
                'patient_weight_original': row['patient_weight_original'],
                'patient_weight_original_unit': row['patient_weight_original_unit'],
                'body_surface_area': row['body_surface_area'],
                'body_surface_area_unit': row['body_surface_area_unit'],
                'body_surface_area_original': row['body_surface_area_original'],
                'body_surface_area_original_unit': row['body_surface_area_original_unit'],
                'modified_ecog': row['modified_ecog']
            })

    for visit in visit_dict.values():
        print(visit)
        print()

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
