"""

Copyright 2021, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
'''


import sys
import os
import re
from os.path import expanduser
import yaml
import io
from git import Repo
from json import loads as json_loads
import pprint
from createSchemaP3 import build_schema

from common_etl.support import confirm_google_vm, create_clean_target, build_file_list, \
                               upload_to_bucket, csv_to_bq, BucketPuller, build_combined_schema, \
                               install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, publish_table, pull_from_buckets, \
                               generic_bq_harness, delete_table_bq_job


'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.
'''
def main(args):

    if not confirm_google_vm():
        print(
            'This job needs to run on a Google Cloud Compute Engine '
            'to avoid storage egress charges [EXITING]'
        )
        return

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps = load_config(yaml_file.read())

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    hold_schema_dict = "{}/{}".format(home, params['HOLD_SCHEMA_DICT'])
    hold_schema_list = "{}/{}".format(home, params['HOLD_SCHEMA_LIST'])

    print('local_files_dir: ', local_files_dir)
    print('hold_schema_dict: ', hold_schema_dict)
    print('hold_schema_list: ', hold_schema_list)

    #
    # Parse table lists and add release number
    #

    for t in params['TMP_TABLE_LIST']:
        params['TMP_TABLE_LIST'][t] = params['TMP_TABLE_LIST'][t].format(params['RELEASE'])
    for t in params['TABLE_LIST']:
        params['TABLE_LIST'][t] = params['TABLE_LIST'][t].format(params['RELEASE'])

    pprint.pprint(params['TMP_TABLE_LIST'])
    pprint.pprint(params['TABLE_LIST'])

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    #
    # Schemas and table descriptions are maintained in the github repo:
    #

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        try:
            create_clean_target(params['SCHEMA_REPO_LOCAL'])
            repo = Repo.clone_from(params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_LOCAL'])
            repo.git.checkout(params['SCHEMA_REPO_BRANCH'])
        except Exception as ex:
            print("pull_table_info_from_git failed: {}".format(str(ex)))
            return

    if 'process_git_schemas' in steps:
        print('process_git_schemas')

        # versioned tables
        for t in params['TABLE_LIST']:
            # Where do we dump the schema git repository?
            schema_file = "{}/{}/{}.json".format(
                params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], params['TABLE_LIST'][t]
            )
            full_file_prefix = "{}/{}".format(local_files_dir, params['TABLE_LIST'][t])
            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

        # current tables
        for t in params['TABLE_LIST']:
            # Where do we dump the schema git repository?
            schema_file = "{}/{}/{}.json".format(
                params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], '_'.join([t, 'current'])
            )
            full_file_prefix = "{}/{}".format(local_files_dir, '_'.join([t, 'current']))
            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

    #
    # Load temp tables from raw data
    #

    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        for t in params['TMP_TABLE_LIST']:
            print('table: ', params['TMP_TABLE_LIST'][t])
            bucket_src_url = 'gs://{}/{}/{}.tsv'.format(
                params['WORKING_BUCKET'], params['WORKING_BUCKET_DIR'], params['TMP_TABLE_LIST'][t]
            )
            schema_file = "{}/{}/{}.json".format(
                params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], params['TMP_TABLE_LIST'][t]
            )
            with open(schema_file, mode='r') as schema_fh:
                schema = json_loads(schema_fh.read())
            csv_to_bq(
                schema['schema']['fields'],
                bucket_src_url,
                params['WORKING_DATASET'],
                params['TMP_TABLE_LIST'][t],
                params['BQ_AS_BATCH']
            )

    #
    # Create derived, final tables from temp tables
    #

    if 'create_physical_entity_table' in steps:
        print('create_physical_entity_table')

        physical_entity_sql = '''
          SELECT
            DISTINCT
              ens.source_id AS ensembl_id,
              uni.source_id AS uniprot_id,
              COALESCE(ens.pe_stable_id, uni.pe_stable_id) AS stable_id,
              REGEXP_EXTRACT(COALESCE(ens.pe_name, uni.pe_name), r"^(.*) \[.*\]$") AS name,
              REGEXP_EXTRACT(COALESCE(ens.pe_name, uni.pe_name), r"^.* \[(.*)\]$") AS location
          FROM
            `{0}.{1}.{2}` AS ens
          FULL OUTER JOIN `{0}.{1}.{3}` AS uni
            ON ens.pe_stable_id = uni.pe_stable_id
          WHERE ens.species = 'Homo sapiens'
          ORDER BY stable_id
        '''.format(
            params['WORKING_PROJECT'],
            params['WORKING_DATASET'],
            params['TMP_TABLE_LIST']['ensembl2reactome'],
            params['TMP_TABLE_LIST']['uniprot2reactome']
        )

        generic_bq_harness(
            physical_entity_sql,
            params['WORKING_DATASET'],
            params['TABLE_LIST']['physical_entity'],
            params['BQ_AS_BATCH'],
            True
        )

    if 'create_pathway_table' in steps:
        print('create_pathway_table')

        pathway_sql = '''
          WITH tmp_pathway AS (
            SELECT
              DISTINCT
                COALESCE(ens.pathway_stable_id, uni.pathway_stable_id) AS stable_id,
                COALESCE(ens.url, uni.url) AS url,
                COALESCE(ens.pathway_name, uni.pathway_name) AS name,
                COALESCE(ens.species, uni.species) AS species
              FROM
                `{0}.{1}.{2}` AS ens
              FULL OUTER JOIN `{0}.{1}.{3}` AS uni
                ON ens.pathway_stable_id = uni.pathway_stable_id
              WHERE COALESCE(ens.species, uni.species) = 'Homo sapiens'
              ORDER BY stable_id
          )
          SELECT
            stable_id,
            url,
            name,
            species,
            IF(
              stable_id NOT IN (
                SELECT
                  DISTINCT parent_id
                FROM
                  `{0}.{1}.{4}`
              ),
              TRUE,
              FALSE
            ) AS lowest_level
          FROM tmp_pathway
        '''.format(
            params['WORKING_PROJECT'],
            params['WORKING_DATASET'],
            params['TMP_TABLE_LIST']['ensembl2reactome'],
            params['TMP_TABLE_LIST']['uniprot2reactome'],
            params['TMP_TABLE_LIST']['pathways_relation']
        )

        generic_bq_harness(
            pathway_sql,
            params['WORKING_DATASET'],
            params['TABLE_LIST']['pathway'],
            params['BQ_AS_BATCH'],
            True
        )

    if 'create_pe_to_pathway_table' in steps:
        print('create_pe_to_pathway_table')

        pathway_sql = '''
          SELECT
            DISTINCT pe_stable_id, pathway_stable_id, evidence_code
          FROM `{0}.{1}.{2}`
          WHERE species = 'Homo sapiens'
          UNION DISTINCT
          SELECT
            DISTINCT pe_stable_id, pathway_stable_id, evidence_code
          FROM `{0}.{1}.{3}`
          WHERE species = 'Homo sapiens'
        '''.format(
            params['WORKING_PROJECT'],
            params['WORKING_DATASET'],
            params['TMP_TABLE_LIST']['ensembl2reactome'],
            params['TMP_TABLE_LIST']['uniprot2reactome']
        )

        generic_bq_harness(
            pathway_sql,
            params['WORKING_DATASET'],
            params['TABLE_LIST']['pe_to_pathway'],
            params['BQ_AS_BATCH'],
            True
        )

    if 'create_pathway_hierarchy_table' in steps:
        print('create_pathway_hierarchy_table')

        pathway_sql = '''
          SELECT
            DISTINCT parent_id, child_id
          FROM `{0}.{1}.{2}` AS pathway_rel
          INNER JOIN `{0}.{1}.{3}` AS pathway_parent
            ON pathway_rel.parent_id = pathway_parent.stable_id
          INNER JOIN `{0}.{1}.{3}` AS pathway_child
            ON pathway_rel.child_id = pathway_child.stable_id
        '''.format(
            params['WORKING_PROJECT'],
            params['WORKING_DATASET'],
            params['TMP_TABLE_LIST']['pathways_relation'],
            params['TABLE_LIST']['pathway']
        )

        generic_bq_harness(
            pathway_sql,
            params['WORKING_DATASET'],
            params['TABLE_LIST']['pathway_hierarchy'],
            params['BQ_AS_BATCH'],
            True
        )

    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions')
        for t in params['TABLE_LIST']:
            full_file_prefix = "{}/{}".format(local_files_dir, params['TABLE_LIST'][t])
            schema_dict_loc = "{}_schema.json".format(full_file_prefix)
            schema_dict = {}
            with open(schema_dict_loc, mode='r') as schema_hold_dict:
                full_schema_list = json_loads(schema_hold_dict.read())
            for entry in full_schema_list:
                schema_dict[entry['name']] = {'description': entry['description']}

            success = update_schema_with_dict(
                params['WORKING_DATASET'], params['TABLE_LIST'][t], schema_dict
            )
            if not success:
                print("update_field_descriptions failed")
                return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        for t in params['TABLE_LIST']:
            full_file_prefix = "{}/{}".format(local_files_dir, params['TABLE_LIST'][t])
            success = install_labels_and_desc(
                params['WORKING_DATASET'], params['TABLE_LIST'][t], full_file_prefix
            )
            if not success:
                print("update_table_description failed")
                return

    #
    # Delete temp tables
    #

    if 'delete_temp_tables' in steps:
        print('delete_tmp_tables')
        for t in params['TMP_TABLE_LIST']:
            print('Deleting temp table: {}.{}.{}'.format(
                params['WORKING_PROJECT'], params['WORKING_DATASET'], params['TMP_TABLE_LIST'][t]
            ))
            success = delete_table_bq_job(
                params['WORKING_DATASET'], params['TMP_TABLE_LIST'][t], params['WORKING_PROJECT']
            )
            if not success:
                print('delete_temp_tables failed')
                return

    #
    # Publish tables
    #

    if 'publish' in steps:
        print('publish')
        tables = ['versioned', 'current']

        for table in tables:
            for t in params['TABLE_LIST']:
                source_table = '{}.{}.{}'.format(
                    params['WORKING_PROJECT'],
                    params['WORKING_DATASET'],
                    params['TABLE_LIST'][t]
                )
                if table == 'versioned':
                    publication_dest = '{}.{}.{}'.format(
                        params['PUBLICATION_PROJECT'],
                        '_'.join([params['PUBLICATION_DATASET'], 'versioned']),
                        params['TABLE_LIST'][t]
                    )
                else:
                    publication_dest = '{}.{}.{}'.format(
                        params['PUBLICATION_PROJECT'],
                        params['PUBLICATION_DATASET'],
                        '_'.join([t, 'current'])
                    )
 
                print('publish table {} -> {}'.format(source_table, publication_dest))
                success = publish_table(source_table, publication_dest)
                if not success:
                    print('publish table {} -> {} failed'.format(source_table, publication_dest))

    #
    # Update description and labels of the current tables
    #

    if 'update_current_table_description' in steps:
        print('update_current_table_description')
        for t in params['TABLE_LIST']:
            full_file_prefix = "{}/{}".format(local_files_dir, '_'.join([t, 'current']))
            success = install_labels_and_desc(
                params['PUBLICATION_DATASET'],
                '_'.join([t, 'current']),
                full_file_prefix,
                params['PUBLICATION_PROJECT']
            )
            if not success:
                print("update current table description failed")
                return

    #
    # Update previous tables with archived status
    #

    if 'update_status_tag' in steps:
        print('update_status_tag')

        for t in params['TABLE_LIST']:
            success = update_status_tag(
                "_".join([params['PUBLICATION_DATASET'], 'versioned']),
                params['TABLE_LIST'][t],
                'archived',
                params['PUBLICATION_PROJECT']
            )
            if not success:
                print("update status tag of table {} failed".format(t))
                return

    #
    # Delete scratch tables
    #

    if 'delete_scratch_tables' in steps:
        print('delete_scratch_tables')

        for t in params['TABLE_LIST']:
            print('Deleting scratch table: {}.{}.{}'.format(
                params['WORKING_PROJECT'], params['WORKING_DATASET'], params['TABLE_LIST'][t]
            ))
            success = delete_table_bq_job(
                params['WORKING_DATASET'], params['TABLE_LIST'][t], params['WORKING_PROJECT']
            )
            if not success:
                print('delete_scratch_tables failed')
                return

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

