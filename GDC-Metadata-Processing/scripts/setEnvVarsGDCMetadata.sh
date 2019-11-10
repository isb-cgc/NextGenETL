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
# Where TSV files get written on the way to BQ tables:
BUCK_TARGET=gs://your-bucket-name-here/etl/${RELNAME}
# Where the BQ data set lives (project:dataset_name)
DATASET=your-etl-project:your_dataset_name

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

#
# These flags tell the script what to do. This allows the user to do the workflow step-by-step as desired.
# Actual values are set in the setEnvVarsGDCMetadata.sh file, **not here**. This script should, in practice,
# not need to be edited as things change from release to release!
#
# Important! The BUILD_DIR and API_PULL steps (PHASE I) MUST NOT be run with the following steps. The API
# pull steps runs for days, and will exit immediately after nohupping the jobs
#

BUILD_DIR=skip
API_PULL=skip
HEX_EXTRACT=run
CURR_FILE_CHECK=run
LEG_FILE_CHECK=run
QC_CHECK=run
GEN_CUT_LISTS=run
BQ_PREP_CASES=run
BQ_PREP_OTHER=run
RAW_SCHEMA_CHECK=run
COPY_ANNOT_SCHEMA=run
LOAD_BQ=run

