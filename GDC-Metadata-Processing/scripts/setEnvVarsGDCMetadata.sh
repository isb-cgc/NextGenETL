#!/usr/bin/env bash

# Copyright 2019, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set to a release tag (e.g. rel18)
RELNAME=relXX
# Set to previous release if doing comparisons of release changes
PREV_RELNAME=relXXminusOne
# Where TSV files get written on the way to BQ tables:
BUCK_TARGET=gs://your-bucket-name-here/etl/${RELNAME}
# Where the BQ data set lives (dataset_name)
DATASET=your_dataset_name
# Project where the etl work is being done
WORKING_PROJECT=your-etl-project
# Where the BQ data set lives (project:dataset_name)
PROJ_AND_DATASET=${WORKING_PROJECT}:${DATASET}
# Where do we store the compressed tar file of the extracted data:
TAR_TARGET=gs://your-bucket-name-here/metatars/${RELNAME}
# The release ID in BQ Schema repo (might be different than RELNAME):
BQ_SCHEMA_RELNAME=relXX
# The published release ID (might be different than RELNAME):
PUB_RELNAME=relXX
# The publication source:
SOURCE_PROJ_AND_DATASET_AND_REL=${WORKING_PROJECT}.${DATASET}.${RELNAME}
# The publication location:
PUBLISH_PROJ_AND_DATASET_AND_REL=your-publish-project.your-publish-dataset.${PUB_RELNAME}


#
# These have been stable for many releases, so we expect them to stay the
# same. If they change (e.g. by case redaction) mod these here:
#
EXPECTED_LEGACY_FILE_SIZE_BY_CASE=761544
EXPECTED_LEGACY_FILE_SIZE_BY_FILE=837960

#
# Used to decide which columns are kept and tossed. Includes the expected results
# from recent runs. If things change, job will halt to warn you, and you should
# investigate before changing the expected values:
#

# Keep the lines short by building in steps:
KEEP_COLS='case_id project__dbgap_accession_number project__disease_type project__name '
KEEP_COLS=${KEEP_COLS}'project__program__dbgap_accession_number project__program__name '
KEEP_COLS=${KEEP_COLS}'project__project_id submitter_id'

KEEP_COUNT="summary__file_count"
TOSS_COLS="error_type"

EXPECTED_LEGACY="2,5,6,7,8,9,10,28"
EXPECTED_CURRENT="2,5,6,7,8,9,10,29"
EXPECTED_LEGACY_FILE_COUNT_COL="30"
EXPECTED_CURRENT_FILE_COUNT_COL="31"
EXPECTED_ERROR_COL="30"

ALIQUOT_CHANGE_ID_FIELD="14"
SLIDE_CHANGE_ID_FIELD="10"
CASE_CHANGE_ID_FIELD="0"
CURR_FILE_CHANGE_ID_FIELD="1"
LEG_FILE_CHANGE_ID_FIELD="1"

#
# These flags tell the script what to do. This allows the user to do the workflow step-by-step as desired.
#
# Important! The BUILD_DIR and API_PULL_* steps (PHASE I) MUST NOT be run with the following steps. The API
# pull steps runs for days, and will exit immediately after nohupping the jobs
#

BUILD_DIR=run
API_PULL_LEGACY=skip
API_PULL_CURRENT=run
HEX_EXTRACT=skip
CURR_FILE_CHECK=skip
LEG_FILE_CHECK=skip
QC_CHECK=skip
GEN_CUT_LISTS=skip
BQ_PREP_CASES=skip
BQ_PREP_OTHER=skip
RAW_SCHEMA_CHECK=skip
BUILD_NORM_TSVS=skip
COMPARE_TO_LAST=skip
DETAILED_DIFFS=skip
COPY_ANNOT_SCHEMA=skip
LOAD_BQ=skip
DESC_AND_LABELS=skip
PUBLISH_TABLES=skip
ARCHIVE_TARS=skip

