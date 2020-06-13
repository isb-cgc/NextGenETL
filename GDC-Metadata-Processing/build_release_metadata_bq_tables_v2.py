"""

Copyright 2019-2020, Institute for Systems Biology

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

Extract GDC Metadata into Per-Project/Build File BQ Tables
This is still a work in progress (01/18/2020)

'''

import yaml
import sys
import io
from git import Repo
from json import loads as json_loads

from common_etl.support import generic_bq_harness, confirm_google_vm, \
                               bq_harness_with_result, delete_table_bq_job, \
                               bq_table_exists, bq_table_is_empty, create_clean_target, \
                               generate_table_detail_files, customize_labels_and_desc, \
                               update_schema_with_dict, install_labels_and_desc, publish_table

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
        return None, None, None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], 
            yaml_dict['builds'], yaml_dict['build_tags'], yaml_dict['path_tags'],
            yaml_dict['programs'], yaml_dict['schema_tags'])


'''
----------------------------------------------------------------------------------------------
Figure out the number of aliquots present
'''
def extract_aliquot_count(release_table, do_batch):

    sql = extract_aliquot_count_sql(release_table)
    results = bq_harness_with_result(sql, do_batch)
    retval = [row.max_delim for row in results]
    return retval[0] + 1

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_aliquot_count_sql(release_table):

    return '''
            # Count the number of delimeters in the field:
            WITH a1 AS (SELECT file_gdc_id,
                        LENGTH(TRIM(associated_entities__entity_gdc_id)) -
                        LENGTH(TRIM(REPLACE(associated_entities__entity_gdc_id, ';',''))) as delim
            FROM `{0}`
            WHERE (case_gdc_id IS NOT NULL) AND
                  (case_gdc_id NOT LIKE "%;%") AND
                  (case_gdc_id != "multi") AND
                  (associated_entities__entity_type = "aliquot"))
            SELECT MAX(delim) as max_delim FROM a1
            '''.format(release_table)

'''
----------------------------------------------------------------------------------------------
Figure out the programs represented in the data
'''
def extract_program_names(release_table, do_batch):

    sql = extract_program_names_sql(release_table)
    results = bq_harness_with_result(sql, do_batch)
    retval = [row.program_name for row in results if row.program_name is not None and row.program_name != "None"]
    return retval

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_program_names_sql(release_table):
    return '''
        SELECT DISTINCT program_name FROM `{0}` # program_name
        '''.format(release_table)


'''
----------------------------------------------------------------------------------------------
Figure out what programs have valid aliquot mapping data in the table. Not all do; errors in processing
prior to Rel22 for programs with GDC-generated portion and analyte IDs, and programs with bad case
data (ORGANOID-PANCREATIC) will not have useful aliquot mapping data available.
'''
def extract_active_aliquot_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_active_aliquot_file_data_sql(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
In GDC active, there are > 450,000 files that have associated_entities as aliquots. In the case of
VCF files, there are two. We need to pull in associated aliquot, sample, and case IDs in the next step.
As always, multi-case entries are skipped.
'''
def extract_active_aliquot_file_data_sql(release_table, program_name):

    return '''
        SELECT
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            CASE WHEN LENGTH(TRIM(a.associated_entities__entity_gdc_id)) -
                      LENGTH(TRIM(REPLACE(a.associated_entities__entity_gdc_id, ";", ""))) >= 1
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^([a-zA-Z0-9-]+);[a-zA-Z0-9-;]+$")
              ELSE a.associated_entities__entity_gdc_id
            END as aliquot_id_one,
            CASE WHEN LENGTH(TRIM(a.associated_entities__entity_gdc_id)) -
                      LENGTH(TRIM(REPLACE(a.associated_entities__entity_gdc_id, ";", ""))) >= 1
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^[a-zA-Z0-9-]+;([a-zA-Z0-9-]+).*$")
              ELSE CAST(null AS STRING)
            END as aliquot_id_two,
            CASE WHEN LENGTH(TRIM(a.associated_entities__entity_gdc_id)) -
                      LENGTH(TRIM(REPLACE(a.associated_entities__entity_gdc_id, ";", ""))) >= 2
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^[a-zA-Z0-9-]+;[a-zA-Z0-9-]+;([a-zA-Z0-9-]+).*$")
              ELSE CAST(null AS STRING)
            END as aliquot_id_three,
            CASE WHEN LENGTH(TRIM(a.associated_entities__entity_gdc_id)) -
                      LENGTH(TRIM(REPLACE(a.associated_entities__entity_gdc_id, ";", ""))) = 3
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^[a-zA-Z0-9-]+;[a-zA-Z0-9-]+;[a-zA-Z0-9-]+;([a-zA-Z0-9-]+)$")
              ELSE CAST(null AS STRING)
            END as aliquot_id_four,
            a.project_short_name, # TCGA-OV
            # Take everything after first hyphen, including following hyphens:
            CASE WHEN (a.project_short_name LIKE '%-%') THEN
                   REGEXP_EXTRACT(project_short_name, r"^[A-Z0-9\.]+-(.+$)")
                 ELSE
                   CAST(null AS STRING)
            END as project_short_name_suffix, # not always disease code anymore, traditionally e.g. "OV"
            a.program_name, # TCGA
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            a.index_file_gdc_id as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{1}` AS a
        WHERE (a.program_name = "{0}") AND
              (a.case_gdc_id IS NOT NULL) AND
              (a.case_gdc_id NOT LIKE "%;%") AND
              (a.case_gdc_id != "multi") AND
              (a.associated_entities__entity_type = "aliquot")
        '''.format(program_name, release_table)

'''
----------------------------------------------------------------------------------------------
Get two rows from one where there are two aliquots present
'''
def expand_active_aliquot_file_data(aliquot_table, target_dataset, dest_table, do_batch):

    sql = expand_active_aliquot_file_data_sql(aliquot_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
With files that have two aliquots, we want to make one row per aliquot
'''
def expand_active_aliquot_file_data_sql(aliquot_table):

    return '''
        SELECT
            file_gdc_id,
            case_gdc_id,
            aliquot_id_one as aliquot_id,
            project_short_name,
            project_short_name_suffix,
            program_name,
            data_type,
            data_category,
            experimental_strategy,
            file_type,
            file_size,
            data_format,
            platform,
            file_name_key,
            index_file_id,
            index_file_name_key,
            index_file_size,
            access,
            acl
        FROM `{0}`
        UNION ALL
        SELECT
            file_gdc_id,
            case_gdc_id,
            aliquot_id_two as aliquot_id,
            project_short_name,
            project_short_name_suffix,
            program_name,
            data_type,
            data_category,
            experimental_strategy,
            file_type,
            file_size,
            data_format,
            platform,
            file_name_key,
            index_file_id,
            index_file_name_key,
            index_file_size,
            access,
            acl
        FROM `{0}`
        WHERE (aliquot_id_two IS NOT NULL)
        UNION ALL
        SELECT
            file_gdc_id,
            case_gdc_id,
            aliquot_id_three as aliquot_id,
            project_short_name,
            project_short_name_suffix,
            program_name,
            data_type,
            data_category,
            experimental_strategy,
            file_type,
            file_size,
            data_format,
            platform,
            file_name_key,
            index_file_id,
            index_file_name_key,
            index_file_size,
            access,
            acl
        FROM `{0}`
        WHERE (aliquot_id_three IS NOT NULL)
        UNION ALL
        SELECT
            file_gdc_id,
            case_gdc_id,
            aliquot_id_four as aliquot_id,
            project_short_name,
            project_short_name_suffix,
            program_name,
            data_type,
            data_category,
            experimental_strategy,
            file_type,
            file_size,
            data_format,
            platform,
            file_name_key,
            index_file_id,
            index_file_name_key,
            index_file_size,
            access,
            acl
        FROM `{0}`
        WHERE (aliquot_id_four IS NOT NULL)
        '''.format(aliquot_table)

'''
----------------------------------------------------------------------------------------------
Slide extraction
'''
def extract_slide_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql_slides(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''

def extract_file_data_sql_slides(release_table, program_name):

    #
    # If dealing with legacy TCGA slides, some do not even have a program name or case_id. We need to haul those
    # out and parse the file name instead in a repair step.
    #

    optional_program = "" if program_name is None else "(a.program_name = '{0}') AND (a.case_gdc_id IS NOT NULL) AND ".format(program_name)

    print("optional program: {}".format(optional_program))
    return '''
        SELECT
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            a.associated_entities__entity_gdc_id as slide_id,
            a.project_short_name, # TCGA-OV
            # Take everything after first hyphen, including following hyphens:
            CASE WHEN (a.project_short_name LIKE '%-%') THEN
                   REGEXP_EXTRACT(project_short_name, r"^[A-Z0-9\.]+-(.+$)")
                 ELSE
                   CAST(null AS STRING)
            END as project_short_name_suffix, # not always disease code anymore, traditionally e.g. "OV"
            a.program_name, # TCGA
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            a.index_file_gdc_id as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl,
            # Some legacy entries have no case ID or sample ID, it is embedded in the file name, and
            # we need to pull that out to get that info
            CASE WHEN (a.case_gdc_id IS NULL) THEN
                   REGEXP_EXTRACT(a.file_name, r"^([A-Z0-9-]+).+$")
                ELSE
                   CAST(null AS STRING)
            END as slide_barcode
        FROM `{1}` AS a
        WHERE {0} # Omit some conditions if we need to capture rows to repair
              (((a.case_gdc_id NOT LIKE "%;%") AND
               # the second condition captures repair rows
                (a.case_gdc_id != "multi")) OR (a.case_gdc_id IS NULL)) AND
              ((a.data_format = "SVS") OR # catches legacy
               (a.associated_entities__entity_type = "slide")) # catches active
        '''.format(optional_program, release_table)


'''
----------------------------------------------------------------------------------------------
Slide repair. Legacy archive is full of bogus slide files which only can be identified by the file name.
These tables do not hold the case id, nor the program or disease name. Fix this bogosity!
'''
def repair_slide_file_data(case_table, broken_table, target_dataset, dest_table, do_batch):

    sql = repair_missing_case_data_sql_slides(case_table, broken_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above. Note this processing ends up throwing away one slide from case TCGA-08-0384, which
actually does not appear in the case file going back to at least release 6.
'''

def repair_missing_case_data_sql_slides(case_table, broken_table):

    return '''
        WITH
           a1 AS
          (SELECT case_gdc_id, project_id, case_barcode,
             REGEXP_EXTRACT(project_id, r"^[A-Z]+-([A-Z]+$)") as project_short_name_suffix,
             program_name # TCGA
           FROM `{0}`
           ),
          a2 AS
          (SELECT *
           FROM `{1}` WHERE slide_barcode IS NOT NULL
          )
        SELECT
            a2.file_gdc_id,
            a1.case_gdc_id,
            a2.slide_id,
            a1.project_id as project_short_name,
            a1.project_short_name_suffix,
            a1.program_name,
            a2.data_type,
            a2.data_category,
            a2.experimental_strategy,
            a2.file_type,
            a2.file_size,
            a2.data_format,
            a2.platform,
            a2.file_name_key,
            a2.index_file_id,
            a2.index_file_name_key,
            a2.index_file_size,
            a2.access,
            a2.acl,
            a2.slide_barcode
        FROM a2 JOIN a1 ON a1.case_barcode = REGEXP_EXTRACT(a2.slide_barcode, r"^(TCGA-[A-Z0-9][A-Z0-9]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9])")
        UNION ALL
        SELECT * FROM `{1}` WHERE slide_barcode IS NULL
        '''.format(case_table, broken_table)


'''
----------------------------------------------------------------------------------------------
Clinical extraction (CLIN and BIO files):
'''


def extract_active_case_file_data(release_table, program_name, target_dataset, dest_table, do_batch):
    sql = extract_active_case_file_data_sql(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
If the associated entity is a case ID, then haul that in:
'''
def extract_active_case_file_data_sql(release_table, program_name):

    return '''
        SELECT
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            a.project_short_name, # TCGA-OV
            # Take everything after first hyphen, including following hyphens:
            CASE WHEN (a.project_short_name LIKE '%-%') THEN
                   REGEXP_EXTRACT(project_short_name, r"^[A-Z0-9\.]+-(.+$)")
                 ELSE
                   CAST(null AS STRING)
            END as project_short_name_suffix, # not always disease code anymore, traditionally e.g. "OV"
            a.program_name, # TCGA
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            a.index_file_gdc_id as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{1}` AS a
        WHERE (a.program_name = "{0}") AND
              (a.case_gdc_id IS NOT NULL) AND
              (a.case_gdc_id NOT LIKE "%;%") AND
              (a.case_gdc_id != "multi") AND
              # Note we depend that a slide is not being sucked in here
              # in the legacy case due to a "case" entity type being present and not a slide.
              # Analysis indicates that is a safe conclusion
              (a.associated_entities__entity_type = "case")
        '''.format(program_name, release_table)

'''
----------------------------------------------------------------------------------------------
Get case barcodes associated with case-tagged files:
'''

def extract_case_barcodes(release_table, case_table, program_name, target_dataset, dest_table, do_batch):

    sql = case_barcodes_sql(release_table, case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def case_barcodes_sql(release_table, case_table, program_name):
    return '''
        WITH
        a1 AS (SELECT DISTINCT case_gdc_id, case_barcode FROM `{1}` GROUP BY case_gdc_id, case_barcode)            
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a1.case_barcode,
            CAST(null AS STRING) as sample_gdc_id,
            CAST(null AS STRING) as sample_barcode,
            CAST(null AS STRING) as sample_type_name,
            a.project_short_name,
            a.project_short_name_suffix,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN a1 ON a.case_gdc_id = a1.case_gdc_id
        '''.format(release_table, case_table, program_name)

'''
----------------------------------------------------------------------------------------------
Get case barcode associated with aliquot-associated files, with no aliqout mapping table data:
'''
def prepare_aliquot_without_map(release_table, case_table, program_name, target_dataset, dest_table, do_batch):

    sql = aliquot_barcodes_without_map_sql(release_table, case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
If we do not have any aliquot mapping, we cannot back it out to sample IDs. So we need to just
get the case barcode and call it good. Note this is a patch for Rel21 and older data, where
metadata processing code was broken.

'''
def aliquot_barcodes_without_map_sql(release_table, case_table, program_name):

    return '''
        SELECT
                d.file_gdc_id,
                d.case_gdc_id,
                e.case_barcode,
                CAST(null AS STRING) as sample_gdc_id,
                CAST(null AS STRING) as sample_barcode,
                CAST(null AS STRING) as sample_type_name,
                d.project_short_name,
                d.project_short_name_suffix,
                d.program_name,
                d.data_type,
                d.data_category,
                d.experimental_strategy,
                d.file_type,
                d.file_size,
                d.data_format,
                d.platform,
                d.file_name_key,
                d.index_file_id,
                d.index_file_name_key,
                d.index_file_size,
                d.access,
                d.acl
        FROM `{0}` AS d JOIN `{1}` AS e ON d.case_gdc_id = e.case_gdc_id
        '''.format(release_table, case_table, program_name)

'''
----------------------------------------------------------------------------------------------
Get sample and case barcodes associated with aliquot-associated files:
'''
def extract_aliquot_barcodes(release_table, aliquot_2_case_table, program_name, target_dataset, dest_table, do_batch):

    sql = aliquot_barcodes_sql(release_table, aliquot_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for getting case and sample info from aliquot IDs.
BAD NEWS! As of Rel21, the aliquot mapping table only holds data for: CGCI, HCMI, TCGA, CPTAC, TARGET,
CCLE. Errors in the code that built the aliquot table meant that many projects did not get loaded. This
problem should mostly be fixed in Rel22, though there appear to be problems with the ORGANOID-PANCREATIC
case data for Rel22.

'''
def aliquot_barcodes_sql(release_table, aliquot_2_case_table, program_name):

    return '''
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            c.case_barcode,
            c.sample_gdc_id,
            c.sample_barcode,
            c.sample_type_name,
            a.project_short_name,
            a.project_short_name_suffix,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN `{1}` AS c ON a.aliquot_id = c.aliquot_gdc_id WHERE a.aliquot_id != "multi"
        # Those cases with a "multi" as an aliquot id, we still need to just haul them in,
        # but also get the case barcode as well:
        UNION DISTINCT
        SELECT
                d.file_gdc_id,
                d.case_gdc_id,
                e.case_barcode,
                CAST(null AS STRING) as sample_gdc_id,
                CAST(null AS STRING) as sample_barcode,
                CAST(null AS STRING) as sample_type_name,
                d.project_short_name,
                d.project_short_name_suffix,
                d.program_name,
                d.data_type,
                d.data_category,
                d.experimental_strategy,
                d.file_type,
                d.file_size,
                d.data_format,
                d.platform,
                d.file_name_key,
                d.index_file_id,
                d.index_file_name_key,
                d.index_file_size,
                d.access,
                d.acl
        FROM `{0}` AS d JOIN `{1}` AS e ON d.case_gdc_id = e.case_gdc_id WHERE d.aliquot_id = "multi"
        '''.format(release_table, aliquot_2_case_table, program_name)

'''
----------------------------------------------------------------------------------------------
Get sample and case barcodes associated with the slide files:
'''
def extract_slide_barcodes(release_table, slide_2_case_table, program_name, target_dataset, dest_table, do_batch):

    sql = slide_barcodes_sql(release_table, slide_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def slide_barcodes_sql(release_table, slide_2_case_table, program_name):

    return '''
        # Some slides have two or more entries in the slide_2_case table if they depict multiple portions. Remove the dups:
        WITH a1 as (
        SELECT DISTINCT
            case_barcode,
            sample_gdc_id,
            sample_barcode,
            sample_type_name,
            slide_gdc_id,
            slide_barcode
        FROM `{1}` GROUP BY case_barcode, sample_gdc_id, sample_barcode, sample_type_name, slide_gdc_id, slide_barcode)
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a1.case_barcode,
            a1.sample_gdc_id as sample_gdc_id,
            a1.sample_barcode as sample_barcode,
            a1.sample_type_name as sample_type_name,
            a.project_short_name,
            a.project_short_name_suffix,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN a1 ON a.slide_barcode = a1.slide_barcode
        UNION DISTINCT
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a1.case_barcode,
            a1.sample_gdc_id as sample_gdc_id,
            a1.sample_barcode as sample_barcode,
            a1.sample_type_name as sample_type_name,
            a.project_short_name,
            a.project_short_name_suffix,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN a1 ON a.slide_id = a1.slide_gdc_id
        '''.format(release_table, slide_2_case_table, program_name)

'''
----------------------------------------------------------------------------------------------
Glue different tables together:
'''
def build_union(table_list, target_dataset, dest_table, do_batch):

    sql = union_sql(table_list)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def union_sql(table_list):
    terms = []
    for table in table_list:
        terms.append("SELECT * FROM `{0}`".format(table))
    filter_term = " UNION ALL ".join(terms)
    return filter_term

'''
----------------------------------------------------------------------------------------------
Final Step:
Get the URIs in from the manifest file:
Two passes used, first for all files, second for index files:
'''
def install_uris(union_table, mapping_table, target_dataset, dest_table, do_batch):
    
    sql = install_uris_sql(union_table, mapping_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def install_uris_sql(union_table, mapping_table):
    return '''
        WITH a1 as (
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a.case_barcode,
            a.sample_gdc_id,
            a.sample_barcode,
            a.sample_type_name,
            a.project_short_name,
            a.project_short_name_suffix,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            c.gcs_path as file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        # THIS VERSION RETAINS THE GDC FILES THAT DO NOT EXIST IN DCF MANIFEST:
        # FROM `{0}` AS a LEFT OUTER JOIN `{1}` AS c ON a.file_gdc_id = c.file_uuid )
        # THIS VERSION DUMPS THE GDC ENTRIES THAT DO NOT EXIST IN DCF MANIFEST:
        FROM `{0}` AS a INNER JOIN `{1}` AS c ON a.file_gdc_id = c.file_uuid )
        
        SELECT
            a1.file_gdc_id,
            a1.case_gdc_id,
            a1.case_barcode,
            a1.sample_gdc_id,
            a1.sample_barcode,
            a1.sample_type_name,
            a1.project_short_name,
            a1.project_short_name_suffix,
            a1.program_name,
            a1.data_type,
            a1.data_category,
            a1.experimental_strategy,
            a1.file_type,
            a1.file_size,
            a1.data_format,
            a1.platform,
            a1.file_name_key,
            a1.index_file_id,
            c.gcs_path as index_file_name_key,
            a1.index_file_size,
            a1.access,
            a1.acl
        FROM a1 LEFT OUTER JOIN `{1}` AS c ON a1.index_file_id = c.file_uuid
        '''.format(union_table, mapping_table)

'''
----------------------------------------------------------------------------------------------
Do all the steps for a given dataset and build sequence
'''

def do_dataset_and_build(steps, build, build_tag, path_tag, dataset_tuple,
                         aliquot_map_programs, params, schema_tags):

    file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)

    #
    # Pull stuff from rel:
    #
     
    if 'pull_slides' in steps:
        step_zero_table = "{}_{}_{}".format(dataset_tuple[1], build, params['SLIDE_STEP_0_TABLE'])
        # Hardwired instead of configurable since this is a one-off problem:
        use_project = None if (build_tag == "legacy") and (dataset_tuple[0] == "TCGA") else dataset_tuple[0]
        success = extract_slide_file_data(file_table, use_project, params['TARGET_DATASET'],
                                          step_zero_table, params['BQ_AS_BATCH'])

        if not success:
            print("{} {} pull_slides job failed".format(dataset_tuple[0], build))
            return False

        if bq_table_is_empty(params['TARGET_DATASET'], step_zero_table):
            delete_table_bq_job(params['TARGET_DATASET'], step_zero_table)
            print("{} pull_slide table result was empty: table deleted".format(params['SLIDE_STEP_0_TABLE']))


    if 'repair_slides' in steps:
        step_zero_table = "{}_{}_{}".format(dataset_tuple[1], build, params['SLIDE_STEP_0_TABLE'])
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                     params['TARGET_DATASET'], step_zero_table)

        if bq_table_exists(params['TARGET_DATASET'], step_zero_table):
            step_one_table = "{}_{}_{}".format(dataset_tuple[1], build, params['SLIDE_STEP_1_TABLE'])
            success = repair_slide_file_data(params['CASE_TABLE'], in_table,
                                             params['TARGET_DATASET'], step_one_table, params['BQ_AS_BATCH'])
            if not success:
                print("{} {} repair slides job failed".format(dataset_tuple[0], build))
                return False

    if 'pull_aliquot' in steps:
        step_zero_table = "{}_{}_{}".format(dataset_tuple[1], build, params['ALIQUOT_STEP_0_TABLE'])
        success = extract_active_aliquot_file_data(file_table, dataset_tuple[0], params['TARGET_DATASET'],
                                                   step_zero_table, params['BQ_AS_BATCH'])
        if not success:
            print("{} {} pull_aliquot job failed".format(dataset_tuple[0], build))
            return False

        if bq_table_is_empty(params['TARGET_DATASET'], step_zero_table):
            delete_table_bq_job(params['TARGET_DATASET'], step_zero_table)
            print("{} pull_aliquot table result was empty: table deleted".format(params['ALIQUOT_STEP_0_TABLE']))

    if 'expand_aliquots' in steps:
        step_zero_table = "{}_{}_{}".format(dataset_tuple[1], build, params['ALIQUOT_STEP_0_TABLE'])
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                     params['TARGET_DATASET'], step_zero_table)

        if bq_table_exists(params['TARGET_DATASET'], step_zero_table):
            step_one_table = "{}_{}_{}".format(dataset_tuple[1], build, params['ALIQUOT_STEP_1_TABLE'])

            success = expand_active_aliquot_file_data(in_table, params['TARGET_DATASET'],
                                                      step_one_table, params['BQ_AS_BATCH'])

            if not success:
                print("{} {} expand_aliquots job failed".format(dataset_tuple[0], build))
                return False

    if 'pull_case' in steps:
        step_one_table = "{}_{}_{}".format(dataset_tuple[1], build, params['CASE_STEP_1_TABLE'])
        success = extract_active_case_file_data(file_table, dataset_tuple[0], params['TARGET_DATASET'],
                                                step_one_table, params['BQ_AS_BATCH'])
        if not success:
            print("{} {} pull_clinbio job failed".format(dataset_tuple[0], build))
            return False

        if bq_table_is_empty(params['TARGET_DATASET'], step_one_table):
            delete_table_bq_job(params['TARGET_DATASET'], step_one_table)
            print("{} pull_case table result was empty: table deleted".format(params['CASE_STEP_1_TABLE']))

    if 'slide_barcodes' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['SLIDE_STEP_1_TABLE'])
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                     params['TARGET_DATASET'], table_name)

        if bq_table_exists(params['TARGET_DATASET'], table_name):
            step_two_table = "{}_{}_{}".format(dataset_tuple[1], build, params['SLIDE_STEP_2_TABLE'])
            success = extract_slide_barcodes(in_table, params['SLIDE_TABLE'], dataset_tuple[0], params['TARGET_DATASET'],
                                             step_two_table, params['BQ_AS_BATCH'])

            if not success:
                print("{} {} slide_barcodes job failed".format(dataset_tuple[0], build))
                return False
        
    if 'aliquot_barcodes' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['ALIQUOT_STEP_1_TABLE'])
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                     params['TARGET_DATASET'], table_name)

        if bq_table_exists(params['TARGET_DATASET'], table_name):
            step_two_table = "{}_{}_{}".format(dataset_tuple[1], build, params['ALIQUOT_STEP_2_TABLE'])

            if dataset_tuple[0] in aliquot_map_programs:
                success = extract_aliquot_barcodes(in_table, params['ALIQUOT_TABLE'], dataset_tuple[0], params['TARGET_DATASET'],
                                                   step_two_table, params['BQ_AS_BATCH'])

                if not success:
                    print("{} {} align_barcodes job failed".format(dataset_tuple[0], build))
                    return False
            else:
                success = prepare_aliquot_without_map(in_table, params['CASE_TABLE'], dataset_tuple[0], params['TARGET_DATASET'],
                                                      step_two_table, params['BQ_AS_BATCH'])

                if not success:
                    print("{} {} align_barcodes job failed".format(dataset_tuple[0], build))
                    return False


        else:
            print("{} {} aliquot_barcodes step skipped (no input table)".format(dataset_tuple[0], build))

    if 'case_barcodes' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['CASE_STEP_1_TABLE'])
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], table_name)

        if bq_table_exists(params['TARGET_DATASET'], table_name):
            step_two_table = "{}_{}_{}".format(dataset_tuple[1], build, params['CASE_STEP_2_TABLE'])
            success = extract_case_barcodes(in_table, params['CASE_TABLE'], dataset_tuple[0], params['TARGET_DATASET'],
                                            step_two_table, params['BQ_AS_BATCH'])

            if not success:
                print("{} {} case_barcodes job failed".format(dataset_tuple[0], build))
                return False

    if 'union_tables' in steps:
        table_list = []

        union_table_tags = ['SLIDE_STEP_2_TABLE', 'ALIQUOT_STEP_2_TABLE', 'CASE_STEP_2_TABLE']

        for tag in union_table_tags:
            if tag in params:
                table_name = "{}_{}_{}".format(dataset_tuple[1], build, params[tag])
                if bq_table_exists(params['TARGET_DATASET'], table_name):
                    full_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], table_name)
                    table_list.append(full_table)

        union_table = "{}_{}_{}".format(dataset_tuple[1], build, params['UNION_TABLE'])
        success = build_union(table_list,
                              params['TARGET_DATASET'], union_table, params['BQ_AS_BATCH'])
        if not success:
            print("{} {} union_tables job failed".format(dataset_tuple[0], build))
            return False

    # Merge the URL info into the final table we are building:

    if 'create_final_table' in steps:
        union_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['TARGET_DATASET'], 
                                        "{}_{}_{}".format(dataset_tuple[1], build, params['UNION_TABLE']))
        success = install_uris(union_table, "{}{}".format(params['UUID_2_URL_TABLE'], path_tag),
                               params['TARGET_DATASET'], 
                               "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE']), params['BQ_AS_BATCH'])
        if not success:
            print("{} {} create_final_table job failed".format(dataset_tuple[0], build))
            return False

    # Stage the schema metadata from the repo copy:

    if 'process_git_schemas' in steps:
        print('process_git_schema')
        # Where do we dump the schema git repository?
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'],
                                        params['GENERIC_SCHEMA_FILE_NAME'])
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_name)
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return False

    # Customize generic schema to this data program:

    if 'replace_schema_tags' in steps:
        print('replace_schema_tags')
        tag_map_list = []
        for tag_pair in schema_tags:
            for tag in tag_pair:
                val = tag_pair[tag]
                use_pair = {}
                tag_map_list.append(use_pair)
                if val.find('~-') == 0 or val.find('~lc-') == 0 or val.find('~lcbqs-') == 0:
                    chunks = val.split('-', 1)
                    if chunks[1] == 'programs':
                        if val.find('~lcbqs-') == 0:
                            rep_val = dataset_tuple[1].lower() # can't have "." in a tag...
                        else:
                            rep_val = dataset_tuple[0]
                    elif chunks[1] == 'path_tags':
                        rep_val = path_tag
                    elif chunks[1] == 'builds':
                        rep_val = build
                    else:
                        raise Exception()
                    if val.find('~lc-') == 0:
                        rep_val = rep_val.lower()
                    use_pair[tag] = rep_val
                else:
                    use_pair[tag] = val
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_name)
        # Write out the details
        success = customize_labels_and_desc(full_file_prefix, tag_map_list)
        if not success:
            print("replace_schema_tags failed")
            return False

    #
    # Update the per-field descriptions:
    #

    if 'install_field_descriptions' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE'])
        print('install_field_descriptions: {}'.format(table_name))
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_name)
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            schema_dict[entry['name']] = {'description': entry['description']}
        success = update_schema_with_dict(params['TARGET_DATASET'], table_name, schema_dict, project=params['WORKING_PROJECT'])
        if not success:
            print("install_field_descriptions failed")
            return False

    #
    # Add description and labels to the target table:
    #

    if 'install_table_description' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE'])
        print('install_table_description: {}'.format(table_name))
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_name)
        success = install_labels_and_desc(params['TARGET_DATASET'], table_name, full_file_prefix,
                                          project=params['WORKING_PROJECT'])
        if not success:
            print("install_table_description failed")
            return False

    #
    # publish table:
    #

    if 'publish' in steps:
        table_name = "{}_{}_{}".format(dataset_tuple[1], build, params['FINAL_TABLE'])
        print('publish: {}'.format(table_name))

        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], table_name)
        publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], dataset_tuple[1], table_name)

        success = publish_table(source_table, publication_dest)

        if not success:
            print("publish failed")
            return False

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        print('dump_working_tables')
        dump_tables = []
        dump_table_tags = ['SLIDE_STEP_0_TABLE', 'SLIDE_STEP_1_TABLE', 'SLIDE_STEP_2_TABLE',
                           'ALIQUOT_STEP_0_TABLE', 'ALIQUOT_STEP_1_TABLE', 'ALIQUOT_STEP_2_TABLE',
                           'CASE_STEP_1_TABLE', 'CASE_STEP_2_TABLE',
                           'UNION_TABLE']
        for tag in dump_table_tags:
            table_name = "{}_{}_{}".format(dataset_tuple[1], build, params[tag])
            if bq_table_exists(params['TARGET_DATASET'], table_name):
                dump_tables.append(table_name)

        for table in dump_tables:
            success = delete_table_bq_job(params['TARGET_DATASET'], table)
            if not success:
                print("problem deleting table {}".format(table))

    #
    # Done!
    #
    
    return True

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
'''

def main(args):

    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
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
        params, steps, builds, build_tags, path_tags, programs, schema_tags = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    #
    # Schemas and table descriptions are maintained in the github repo. Only do this once:
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

    #
    # The SQL is currently tailored to parse out up to two aliquots per file (understanding that
    # we are only processing files that apply to a single case, and not multiple case files). This
    # step checks that that assumption is not being violated:
    #

    if 'count_aliquots' in steps:
        print('count_aliquots')

        try:
            for build_tag in build_tags:
                file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)
                aliquot_count = extract_aliquot_count(file_table, params['BQ_AS_BATCH'])
                print ("{}:{}".format(build_tag, aliquot_count))
                if aliquot_count > params['MAX_ALIQUOT_PARSE']:
                    print("count_aliquots detected high aliquot count: {} > {}. Exiting.".format(aliquot_count,
                                                                                                 params['MAX_ALIQUOT_PARSE']))
                    return
        except Exception as ex:
            print("count_aliquots failed: {}".format(str(ex)))
            return

    for build, build_tag, path_tag in zip(builds, build_tags, path_tags):
        file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)
        do_programs = extract_program_names(file_table, params['BQ_AS_BATCH']) if programs is None else programs
        dataset_tuples = [(pn, pn.replace(".", "_")) for pn in do_programs] # handles BEATAML1.0 FIXME! Make it general
         # Not all programs show up in the aliquot map table. So figure out who does:
        aliquot_map_programs = extract_program_names(params['ALIQUOT_TABLE'], params['BQ_AS_BATCH'])
        print(dataset_tuples)
        for dataset_tuple in dataset_tuples:
            print ("Processing build {} ({}) for program {}".format(build, build_tag, dataset_tuple[0]))
            ok = do_dataset_and_build(steps, build, build_tag, path_tag, dataset_tuple,
                                      aliquot_map_programs, params, schema_tags)
            if not ok:
                return
            
    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

