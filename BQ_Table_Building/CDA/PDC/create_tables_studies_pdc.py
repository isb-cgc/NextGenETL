"""
Copyright 2023, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import sys
import time
import json
from typing import Optional, Any

from cda_bq_etl.data_helpers import normalize_flat_json_values, write_list_to_jsonl_and_upload, initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, get_filepath, create_metadata_table_id
from cda_bq_etl.bq_helpers import (query_and_retrieve_result, update_table_schema_from_generic,
                                   create_and_upload_schema_for_json, retrieve_bq_schema_object,
                                   create_and_load_table_from_jsonl)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def get_project_metadata():
    """
    Load project metadata from BQEcosystem/MetadataMappings/pdc_project_metadata.json as dict.
    :return dict of project dicts of the following form. Key equals PDC field "project_submitter_id."
    Example project dict:
        { "CPTAC-TCGA": {
            "project_short_name": "CPTAC_TCGA",
            "project_friendly_name": "CPTAC-TCGA",
            "program_short_name": "TCGA",
            "program_labels": "cptac2; tcga"
        }
    """
    metadata_mappings_path = f"{PARAMS['BQ_REPO']}/{PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    project_metadata_fp = get_filepath(f"{metadata_mappings_path}/{PARAMS['PROJECT_METADATA_FILE']}")

    with open(project_metadata_fp, 'r') as fh:
        return json.load(fh)


def get_study_friendly_names():
    """
    Load study friendly names json file (from BQEcosystem/MetadataMappings/pdc_study_friendly_name_map.json) as dict.
    :return: dict of { "pdc_study_id": "STUDY FRIENDLY NAME" } strings
    """
    metadata_mappings_path = f"{PARAMS['BQ_REPO']}/{PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    study_metadata_fp = get_filepath(f"{metadata_mappings_path}/{PARAMS['STUDY_FRIENDLY_NAME_FILE']}")

    with open(study_metadata_fp, 'r') as fh:
        return json.load(fh)


def make_study_query() -> str:
    return f"""
        SELECT 
            s.study_name,
            s.study_submitter_id,
            s.submitter_id_name,
            s.pdc_study_id,
            s.study_id,
            s.analytical_fraction,
            s.acquisition_type,
            s.experiment_type,
            proj.project_id,
            proj.project_submitter_id,
            proj.name AS project_name,
            prog.program_id,
            prog.program_submitter_id,
            prog.name AS program_name,
            prog.program_manager,
            prog.start_date,
            prog.end_date
        FROM `{create_dev_table_id(PARAMS, "study")}` s
        JOIN `{create_dev_table_id(PARAMS, "project_study_id")}` proj_study
            ON s.study_id = proj_study.study_id
        JOIN `{create_dev_table_id(PARAMS, "project")}` proj
            ON proj.project_id = proj_study.project_id
        JOIN `{create_dev_table_id(PARAMS, "program_project_id")}` prog_proj
            ON prog_proj.project_id = proj.project_id
        JOIN `{create_dev_table_id(PARAMS, "program")}` prog
            ON prog.program_id = prog_proj.program_id
    """


def make_study_disease_type_query() -> str:
    return f"""
        SELECT s.pdc_study_id,
            s.study_id,
            STRING_AGG(sdt.disease_type, ';' ORDER BY sdt.disease_type) AS disease_type,
        FROM `{create_dev_table_id(PARAMS, "study")}` s
        LEFT JOIN `{create_dev_table_id(PARAMS, "study_disease_type")}` sdt
            ON s.study_id = sdt.study_id
        GROUP BY s.pdc_study_id, s.study_id
    """


def make_study_primary_site_query() -> str:
    return f"""
        SELECT s.pdc_study_id,
            s.study_id,
            STRING_AGG(sps.primary_site, ';' ORDER BY sps.primary_site) AS primary_site
        FROM `{create_dev_table_id(PARAMS, "study")}` s
        LEFT JOIN `{create_dev_table_id(PARAMS, "study_primary_site")}` sps
            ON s.study_id = sps.study_id
        GROUP BY s.pdc_study_id, s.study_id
    """


def create_study_record_list() -> list[dict[str, Optional[Any]]]:
    logger = logging.getLogger('base_script')

    project_metadata_records = get_project_metadata()
    study_friendly_names = get_study_friendly_names()

    study_record_result = query_and_retrieve_result(sql=make_study_query())

    study_disease_type_result = query_and_retrieve_result(sql=make_study_disease_type_query())
    study_primary_site_result = query_and_retrieve_result(sql=make_study_primary_site_query())

    study_disease_types_records = dict()
    study_primary_site_records = dict()

    for row in study_disease_type_result:
        pdc_study_id = row.get('pdc_study_id')
        study_disease_types_records[pdc_study_id] = dict(row)

    for row in study_primary_site_result:
        pdc_study_id = row.get('pdc_study_id')
        study_primary_site_records[pdc_study_id] = dict(row)

    study_records = list()

    for row in study_record_result:
        project_submitter_id = row.get('project_submitter_id')

        if project_submitter_id == 'CPTAC2 Retrospective':
            project_submitter_id = 'CPTAC-2'

        pdc_study_id = row.get('pdc_study_id')
        study_friendly_name = study_friendly_names[pdc_study_id]
        project_metadata = project_metadata_records[project_submitter_id]

        project_short_name = project_metadata['project_short_name']
        project_friendly_name = project_metadata['project_friendly_name']
        program_short_name = project_metadata['program_short_name']

        if 'program_label' in project_metadata:
            program_labels = project_metadata['program_label']
        elif 'program_label_0' in project_metadata and 'program_label_1' in project_metadata:
            program_labels = f"{project_metadata['program_label_0']}; {project_metadata['program_label_1']}"
        else:
            logger.critical(f"No program labels found for {project_submitter_id} in {PARAMS['PROJECT_METADATA_FILE']}.")
            sys.exit(-1)

        disease_type = study_disease_types_records[pdc_study_id]['disease_type']
        primary_site = study_primary_site_records[pdc_study_id]['primary_site']

        study_records.append({
            'study_name': row.get('study_name'),
            'study_submitter_id': row.get('study_submitter_id'),
            'submitter_id_name': row.get('submitter_id_name'),
            'pdc_study_id': row.get('pdc_study_id'),
            'study_id': row.get('study_id'),
            'study_friendly_name': study_friendly_name,
            'analytical_fraction': row.get('analytical_fraction'),
            'disease_type': disease_type,
            'primary_site': primary_site,
            'acquisition_type': row.get('acquisition_type'),
            'experiment_type': row.get('experiment_type'),
            'project_id': row.get('project_id'),
            'project_submitter_id': project_submitter_id,
            'project_name': row.get('project_name'),
            'project_short_name': project_short_name,
            'project_friendly_name': project_friendly_name,
            'program_id': row.get('program_id'),
            'program_submitter_id': row.get('program_submitter_id'),
            'program_name': row.get('program_name'),
            'program_short_name': program_short_name,
            'program_manager': row.get('program_manager'),
            'program_labels': program_labels,
            'start_date': row.get('start_date'),
            'end_date': row.get('end_date')
        })

    return study_records


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
    
    dev_table_id = create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME'])

    if 'create_and_upload_study_jsonl' in steps:
        logger.info("Entering create_and_upload_study_jsonl")

        study_records = create_study_record_list()
        normalized_study_records = normalize_flat_json_values(study_records)

        write_list_to_jsonl_and_upload(PARAMS, 'study', normalized_study_records)
        write_list_to_jsonl_and_upload(PARAMS, 'study_raw', study_records)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=normalized_study_records,
                                          table_name='study',
                                          include_release=True)

    if 'create_table' in steps:
        logger.info("Entering create_table")

        # Download schema file from Google Cloud bucket
        table_schema = retrieve_bq_schema_object(PARAMS, table_name='study', include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"study_{PARAMS['RELEASE']}.jsonl",
                                         table_id=dev_table_id,
                                         schema=table_schema)

        update_table_schema_from_generic(params=PARAMS, table_id=dev_table_id)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
