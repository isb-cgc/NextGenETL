"""

Copyright 2019-2024, Institute for Systems Biology

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

import logging
import sys
import os
import time
from git import Repo
import requests
import urllib.parse as up
from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import bigquery, storage
import shutil
import re
from distutils import util
from json import loads as json_loads, dumps as json_dumps

# Initiate logger
util_logger = logging.getLogger(name='base_script.util')

# General Utilities #

def format_seconds(seconds):
    """
    Round seconds to formatted hour, minute, and/or second output.
    Function originally from utils.py by L Wolfe
    :param seconds: int representing time in seconds
    :return: formatted time string
    """
    if seconds > 3600:
        return time.strftime("%-H hours, %-M minutes, %-S seconds", time.gmtime(seconds))
    if seconds > 60:
        return time.strftime("%-M minutes, %-S seconds", time.gmtime(seconds))

    return time.strftime("%-S seconds", time.gmtime(seconds))


def initialize_logging(log_filepath: str) -> logging.Logger:
    # initialize Logger object
    # Function originally from NextGenETL/cda_bq_etl/data_helpers.py by Lauren W on 11/20/23
    logger = logging.getLogger(name='base_script')
    logger.setLevel(logging.DEBUG)

    # emit logger output to a file
    log_file_handler = logging.FileHandler(log_filepath)
    log_file_handler.setLevel(logging.DEBUG)

    # emit logger output to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # format log output: levelname is the severity level, e.g. INFO, WARNING
    # name is the location from which the message was emitted; lineno is the line
    formatter = logging.Formatter('[%(levelname)s][%(name)s:%(lineno)s] %(message)s')
    log_file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(log_file_handler)
    logger.addHandler(console_handler)

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    logger.info(f"\n---\nLogging started: {start_time}")

    return logger


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Ripped from Stack Overflow.
    https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    H/T to Greenstick: https://stackoverflow.com/users/2206251/greenstick

    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()
    return


# File Utilities #


def clean_local_file_dir(local_files_dir):
    """
    This routine clears the tree out if it exists.
    Function originally from support.py called 'create_clean_target'
    """

    if os.path.exists(local_files_dir):
        util_logger.info(f"deleting {local_files_dir}")
        try:
            shutil.rmtree(local_files_dir)
        except OSError as e:
            util_logger.info(f"Error: {e.filename} - {e.strerror}.")

        util_logger.info(f"done {local_files_dir}")

    if not os.path.exists(local_files_dir):
        os.makedirs(local_files_dir)


def get_column_list_tsv(header_list=None, tsv_fp=None, header_row_index=None):
    """
    Return a list of column headers using header_list OR using a header_row index to retrieve column names from tsv_fp.
    Function originally from utils.pyh called 'get_column_list_tsv'
        NOTE: Specifying both header_list and header_row in parent function triggers a fatal error.
    :param header_list: Optional ordered list of column headers corresponding to columns in dataset tsv file
    :type header_list: list
    :param tsv_fp: Optional string filepath; provided if column names are being obtained directly from tsv header
    :type tsv_fp: str
    :param header_row_index: Optional header row index, if deriving column names from tsv file
    :type header_row_index: int
    :return list of columns with BQ-compatible names
    :rtype list
    """

    if not header_list and not header_row_index and not isinstance(header_row_index, int):
        util_logger.error("Must supply either the header row index or header list for tsv schema creation.")
        sys.exit()
    if header_row_index and header_list:
        util_logger.error("Can't supply both a header row index and header list for tsv schema creation.")
        sys.exit()

    column_list = list()

    if header_list:
        for column in header_list:
            column_list.append(column)
    else:
        with open(tsv_fp, 'r') as tsv_file:
            if header_row_index:
                for index in range(header_row_index):
                    tsv_file.readline()

            column_row = tsv_file.readline()
            columns = column_row.split('\t')

            if len(columns) == 0:
                util_logger.error("No column name values supplied by header row index")
                sys.exit()

            for column in columns:
                column_list.append(column)

    return column_list


def build_file_list(local_files_dir):
    """
    Build the File List
    Using the tree of downloaded files, we build a file list. Note that we see the downloads
    (using the GDC download tool) bringing along logs and annotation.txt files, which we
    specifically omit.
    Function originally from support.py called 'build_file_list'
    """
    print(f"building file list from {local_files_dir}")
    all_files = []
    for path, dirs, files in os.walk(local_files_dir):
        if not path.endswith('logs'):
            for f in files:
                if f != 'annotations.txt':
                    if f.endswith('parcel'):
                        raise Exception
                    all_files.append(f'{path}/{f}')

    print(f"done building file list from {local_files_dir}") # todo logger
    return all_files


def update_dir_from_git(local_repo, repo_url, repo_branch):
    """
    This function deletes the old directory and replaces it with the most current from GitHub
    Function originally from support.py called 'update_dir_from_git'
    :param local_repo: Where the local directory for the repository is
    :type local_repo: str
    :param repo_url: The URL for the directory to clone
    :type repo_url: str
    :param repo_branch: The branch to use for the repository
    :type repo_branch: str
    :return: Whether the function worked or not
    :rtype: bool
    """
    try:
        clean_local_file_dir(local_repo)
        repo = Repo.clone_from(repo_url, local_repo)
        repo.git.checkout(repo_branch)
        util_logger.info(f"{local_repo} was updated from GitHub")
        return True
    except Exception as ex:
        util_logger.error(f"update_dir_from_git failed: {str(ex)}")
        return False


def bucket_to_local(bucket_name, bucket_file, local_file):
    """
    Get a Bucket File to Local
    Export a cloud bucket file to the local filesystem
    No leading / in bucket_file name!!
    Function originally from support.py called 'bucket_to_local'
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)  # no leading / in blob name!!
    blob.download_to_filename(local_file)
    util_logger.info(f"{bucket_file} copied to {local_file}")
    return


def local_to_bucket(bucket, bucket_file, local_file):
    """
    Upload to Google Bucket
    Large files have to be in a bucket for them to be ingested into Big Query. This does this.
    This function is also used to archive files.
    Function originally from support.py called 'local_to_bucket'
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(bucket_file)
    blob.upload_from_filename(local_file)
    util_logger.info(f"{local_file} copied to {bucket_file}")


class BucketPuller(object):
    """
    Multithreaded bucket puller
    Function originally from support.py called 'BucketPuller'
    """

    # todo make it output to logger

    def __init__(self, thread_count):
        self._lock = threading.Lock()
        self._threads = []
        self._total_files = 0
        self._read_files = 0
        self._thread_count = thread_count
        self._bar_bump = 0

    def __str__(self):
        return "BucketPuller"

    def reset(self):
        self._threads.clear()
        self._total_files = 0
        self._read_files = 0
        self._bar_bump = 0

    def pull_from_buckets(self, pull_list, local_files_dir):
        """
          List of all project IDs
        """
        self._total_files = len(pull_list)
        self._bar_bump = self._total_files // 100
        if self._bar_bump == 0:
            self._bar_bump = 1
        size = self._total_files // self._thread_count
        size = size if self._total_files % self._thread_count == 0 else size + 1
        chunks = [pull_list[pos:pos + size] for pos in range(0, self._total_files, size)]
        for i in range(0, self._thread_count):
            if i >= len(chunks):
                break
            th = threading.Thread(target=self._pull_func, args=(chunks[i], local_files_dir))
            self._threads.append(th)

        for i in range(0, len(self._threads)):
            self._threads[i].start()

        for i in range(0, len(self._threads)):
            self._threads[i].join()

        print_progress_bar(self._read_files, self._total_files) # todo change to logger
        return

    def _pull_func(self, pull_list, local_files_dir):
        storage_client = storage.Client()
        for url in pull_list:
            path_pieces = up.urlparse(url)
            dir_name = os.path.dirname(path_pieces.path)
            make_dir = f"{local_files_dir}{dir_name}"
            os.makedirs(make_dir, exist_ok=True)
            bucket = storage_client.bucket(path_pieces.netloc)
            blob = bucket.blob(path_pieces.path[1:])  # drop leading / from blob name
            full_file = f"{local_files_dir}{path_pieces.path}"
            blob.download_to_filename(full_file)
            self._bump_progress()

    def _bump_progress(self):

        with self._lock:
            self._read_files += 1
            if (self._read_files % self._bar_bump) == 0:
                print_progress_bar(self._read_files, self._total_files) # todo change to logger


def pull_from_buckets(pull_list, local_files_dir):
    """
    Run the "Download Client", which now just hauls stuff out of the cloud buckets
    Function originally from support.py called 'pull_from_buckets'
    """

    # Parse the manifest file for uuids, pull out other data too as a sanity check:

    num_files = len(pull_list)
    print(f"Begin {num_files} bucket copies...") # todo make logger
    storage_client = storage.Client()
    copy_count = 0

    for url in pull_list:
        url_list = url.replace("[", "").replace("]", "").replace("'", "").split(", ")
        if len(url_list) == 3:
            gcs_url = url_list[2]
        else:
            gcs_url = url_list[0]

        path_pieces = up.urlparse(gcs_url)
        dir_name = os.path.dirname(path_pieces.path)
        make_dir = f"{local_files_dir}{dir_name}"
        os.makedirs(make_dir, exist_ok=True)
        bucket = storage_client.bucket(path_pieces.netloc)
        blob = bucket.blob(path_pieces.path[1:])  # drop leading / from blob name
        full_file = f"{local_files_dir}{path_pieces.path}"
        blob.download_to_filename(full_file)
        copy_count += 1
        if (copy_count % 10) == 0:
            print_progress_bar(copy_count, num_files) # todo make logger
    print_progress_bar(num_files, num_files) # todo make logger


# Google VM Utils #

def confirm_google_vm():
    # todo
    # Function originally from support.py called 'confirm_google_vm'
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/id"
    meta_header = {"Metadata-Flavor": "Google"}

    try:
        resp = requests.request("GET", metadata_url, headers=meta_header)
    except Exception as ex:
        print("Not a Google VM: {}".format(ex))
        return False

    if resp.status_code == 200:
        return True
    else:
        print("Not a Google VM: {}".format(resp.status_code))
        return False


# BQ Utils #

def bq_table_exists(table_id, dataset_id=None, project=None):
    """
    Does the BQ table exist?
    Function originally from support.py called 'bq_table_exists'
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    try:
        if dataset_id is None:
            client.get_table(table_id)
        else:
            client.dataset(dataset_id).table(table_id)
        return True

    except NotFound:
        return False


def copy_bq_table(source_table_id, dest_table_id, project=None, overwrite=False):
    """
    Copy an existing BQ table to another BQ table with an option to remove and replace the older table
    Function adapted from publish_table in support.py and copy_bq_table in utils.py
    :param source_table_id: BQ table id to be copied
    :param dest_table_id: Name of the new BQ table id
    :param project: Google project
    :param overwrite: option to remove old table and replace with the new table
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)

    if overwrite:
        if bq_table_exists(dest_table_id, None, project):
            if not delete_bq_table(dest_table_id):
                sys.exit(f'{dest_table_id} failed to delete')
        else:
            util_logger.info(f"table {dest_table_id} doesn't exists")

    try:
        job = client.copy_table(source_table_id, dest_table_id)
        job.result()
        util_logger.info(f"{source_table_id} copied to {dest_table_id}")

    except Exception as ex:
        util_logger.error(ex)
        sys.exit()


def delete_bq_table(table_id, project=None):
    """
    Delete a BQ table
    Adapted from delete_bq_table in support.py
    :param table_id: BQ table id to be deleted
    :param project: Google project
    :return: that the table was deleted
    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        util_logger.info(f"Deleted table '{table_id}'.")

    except Exception as ex:
        util_logger.error(ex)
        sys.exit()

    return True


def query_bq(sql, dest_table_id=None, project=None):
    """
    Query BQ table and either create a new table or return results
    Adapted from generic_bq_harness_write_depo in support.py
    :param sql: SQL query string
    :param dest_table_id: table to write query output to
    :param project: Google project
    :return: Return the state of the job or the query result
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)

    if dest_table_id is None:
        job_config = bigquery.QueryJobConfig()
    else:
        job_config = bigquery.QueryJobConfig(destination=dest_table_id)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, location='US', job_config=job_config)
    job_state = query_job.state

    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location='US')
        util_logger.info(f'Job {query_job.job_id} is currently in state {query_job.state}')
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    util_logger.info(f'Job {query_job.job_id} is done')

    query_job = client.get_job(query_job.job_id, location='US')
    if query_job.error_result is not None:
        util_logger.error(f'Error result!! {query_job.error_result}')
        return None

    if dest_table_id is None:
        return query_job.result()
    else:
        return query_job.state


def bq_to_bucket_tsv(src_table, project, dataset, bucket_name, bucket_file, do_batch, do_header):
    """
    Get a BQ Result to a Bucket TSV file
    Export BQ table to a cloud bucket
    Function from bq_to_bucket_tsv in support.py
    :param src_table: BQ table name
    :param project: Google Project
    :param dataset: BQ dataset
    :param bucket_name: Google bucket name
    :param bucket_file: File name
    :param do_batch: Should this be batched?
    :param do_header: Should the BQ table header be included in the TSV?
    :return:
    """
    client = bigquery.Client()
    destination_uri = "gs://{}/{}".format(bucket_name, bucket_file)
    dataset_ref = client.dataset(dataset, project=project)
    table_ref = dataset_ref.table(src_table)

    job_config = bigquery.ExtractJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    location = 'US'
    job_config.field_delimiter = '\t'
    job_config.print_header = do_header

    extract_job = client.extract_table(table_ref, destination_uri, location="US", job_config=job_config)

    # Query
    extract_job = client.get_job(extract_job.job_id, location=location)
    job_state = extract_job.state

    while job_state != 'DONE':
        extract_job = client.get_job(extract_job.job_id, location=location)
        util_logger.info('Job {} is currently in state {}'.format(extract_job.job_id, extract_job.state))
        job_state = extract_job.state
        if job_state != 'DONE':
            time.sleep(5)
    util_logger.info('Job {} is done'.format(extract_job.job_id))

    extract_job = client.get_job(extract_job.job_id, location=location)
    if extract_job.error_result is not None:
        util_logger.error(f'Error result!! {extract_job.error_result}')
        return False
    return True


def csv_to_bq(schema, csv_uri, dataset_id, targ_table, do_batch, write_depo):
    """
    Loads a csv file into BigQuery with option to specify disposition
    Function from csv_to_bq in support.py
    :param schema: Dictionary of field name (key) and description (value)
    :type schema: dict
    :param csv_uri: Bucket location of the file in the form of gs://working_bucket/filename.csv
    :type csv_uri: basestring
    :param dataset_id: Name of the dataset where the table will be created
    :type dataset_id: basestring
    :param targ_table: Name of the table to be created
    :type targ_table: basestring
    :param do_batch: Should the BQ job be run in Batch Mode? Slower but uses less quotas
    :type do_batch: bool
    :param write_depo: Should the table be overwritten or appended?
    :type write_depo: class
    :return: Whether the BQ job was completed
    :rtype: bool
    """
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH

    schema_list = []
    for mydict in schema:
        schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                mode='NULLABLE', description=mydict['description']))

    job_config.schema = schema_list
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    if write_depo is not None:
        job_config.write_disposition = write_depo
    # Can make the "CSV" file a TSV file using this:
    job_config.field_delimiter = '\t'

    load_job = client.load_table_from_uri(
        csv_uri,
        dataset_ref.table(targ_table),
        job_config=job_config)  # API request
    util_logger.info(f'Starting job {load_job.job_id}')

    location = 'US'
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)
        util_logger.info(f'Job {load_job.job_id} is currently in state {load_job.state}')
        job_state = load_job.state
        if job_state != 'DONE':
            time.sleep(5)
    util_logger.info(f'Job {load_job.job_id} is done')

    load_job = client.get_job(load_job.job_id, location=location)
    if load_job.error_result is not None:
        util_logger.error(f'Error result!! {load_job.error_result}')
        for err in load_job.errors:
            util_logger.error(err)
        return False

    destination_table = client.get_table(dataset_ref.table(targ_table))
    util_logger.info(f'Loaded {destination_table.num_rows} rows.')
    return True


def cluster_table(input_table_id, output_table_id, cluster_fields):
    """
    CLuster the input table and create a new table
    Function from cluster_table in support.py
    :param input_table_id: Input table ID
    :param output_table_id: output table ID
    :param cluster_fields: fields to cluster on
    :return: job status
    """
    cluster_string = ", ".join(cluster_fields)
    cluster_sql = f"""
          CREATE TABLE `{output_table_id}` 
          CLUSTER BY {cluster_string} 
          AS SELECT * FROM `{input_table_id}`
          """
    return query_bq(cluster_sql)


def find_most_recent_release(dataset, base_table, project=None):
    """
    This function iterates though all tables of a BigQuery versioned dataset to find the most recent release of version
    number of a certain data type. Function from support.py
    Function from find_most_recent_release in support.py
    :param dataset: Dataset to search
    :type dataset: basestring
    :param base_table: The table name before the release number (must include _ before release number)
    :type base_table: basestring
    :param project: Which project is the data set in?
    :type project: basestring
    :returns: The highest version number of that table type in that dataset as a string
    """
    print('finding most recent release in ' + dataset)
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        release = ''  # variable for the most recent release
        table_create = ''  # the most recently created table
        # subset the tables for those that match the desired one
        table_subset = [t for t in client.list_tables(dataset) if t.table_id[:len(base_table)] == base_table]
        if not table_subset:
            print('No older versions to compare to')
            return False
        for t in table_subset:
            # If the table has a newer create date then the one in table_create date, replace current value
            if table_create < str(t.created):
                table_create = str(t.created)
                release = t.table_id[len(base_table):]
    except Exception as ex:
        print(ex)
        return False

    return release


def publish_table(source_table, target_table, overwrite=False):
    """
    Publish table by copying the table
    :param source_table: Source table id
    :param target_table: Target table id
    :param overwrite: Should the old target table be deleted and replaced?
    :return: If the function worked
    """
    try:
        #
        # 3/11/20: Friendly names not copied across, so do it here!
        #

        src_toks = source_table.split('.')
        src_proj = src_toks[0]
        src_dset = src_toks[1]
        src_tab = src_toks[2]

        trg_toks = target_table.split('.')
        trg_proj = trg_toks[0]
        trg_dset = trg_toks[1]
        trg_tab = trg_toks[2]

        src_client = bigquery.Client(src_proj)

        if overwrite:
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = "WRITE_TRUNCATE"
            job = src_client.copy_table(source_table, target_table, job_config=job_config)
            job.result()
        else:
            job = src_client.copy_table(source_table, target_table)
            job.result()

        src_table_ref = src_client.dataset(src_dset).table(src_tab)
        s_table = src_client.get_table(src_table_ref)
        src_friendly = s_table.friendly_name

        trg_client = bigquery.Client(trg_proj)
        trg_table_ref = trg_client.dataset(trg_dset).table(trg_tab)
        t_table = src_client.get_table(trg_table_ref)
        t_table.friendly_name = src_friendly

        trg_client.update_table(t_table, ['friendlyName'])

    except Exception as ex:
        print(ex)
        return False

    return True


def publish_tables_and_update_schema(scratch_table_id, versioned_table_id, current_table_id, release_friendly_name,
                                     base_table=None):
    """
    Publish current and update old versioned
    Function from publish_tables_and_update_schema in support.py
    :param scratch_table_id:
    :param versioned_table_id:
    :param current_table_id:
    :param release_friendly_name:
    :param base_table:
    :return:
    """
    if base_table:
        project, dataset, curr_table = current_table_id.split('.')

        # find the most recent table
        most_recent_release = find_most_recent_release(f"{dataset}_versioned", base_table, project)
        if most_recent_release:
            update_status_tag(f"{dataset}_versioned", f"{base_table}{most_recent_release}", "archived", project)
            # create or update current table and update older versioned tables
            if bq_table_exists(f"{curr_table}", dataset, project):
                print(f"Deleting old table: {current_table_id}")
                if not delete_bq_table(f"{dataset}.{curr_table}", project):
                    sys.exit(f'deleting old current table called {current_table_id} failed')

        print("Creating new current table")
        if not publish_table(scratch_table_id, current_table_id):
            sys.exit(f'creating a new current table called {current_table_id} failed')

    # publish versioned
    print(f"publishing {versioned_table_id}")
    if publish_table(scratch_table_id, versioned_table_id):
        print("Updating friendly name")
        client = bigquery.Client()
        table = client.get_table(versioned_table_id)
        friendly_name = table.friendly_name
        table.friendly_name = f"{friendly_name} {release_friendly_name} VERSIONED"
        client.update_table(table, ["friendly_name"])
    else:
        sys.exit(f'versioned publication failed for {versioned_table_id}')

    return True


# Schema Utils #


def aggregate_column_data_types_tsv(tsv_fp, column_headers, skip_rows, sample_interval=1):
    """
    Open tsv file and aggregate data types for each column.
    Function from aggregate_column_data_types_tsv in utils.py
    :param tsv_fp: tsv dataset filepath used to analyze the data types
    :type tsv_fp: str
    :param column_headers: list of ordered column headers
    :type column_headers: list
    :param skip_rows: number of (header) rows to skip before starting analysis
    :type skip_rows: int
    :param sample_interval: sampling interval, used to skip rows in large datasets; defaults to checking every row
        ex.: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return dict of column keys, with value sets representing all data types found for that column
    :rtype dict {str: set}
    """
    data_types_dict = dict()

    for column in column_headers:
        data_types_dict[column] = set()

    with open(tsv_fp, 'r') as tsv_file:
        for i in range(skip_rows):
            tsv_file.readline()

        count = 0

        while True:
            row = tsv_file.readline()

            if not row:
                break

            if count % sample_interval == 0:
                row_list = row.split('\t')

                for idx, value in enumerate(row_list):
                    value = value.strip()
                    value_type = check_value_type(value)
                    data_types_dict[column_headers[idx]].add(value_type)

            count += 1

    return data_types_dict


def check_value_type(value):
    """
    Check value for corresponding BigQuery type. Evaluates the following BigQuery column data types:
        - datetime formats: DATE, TIME, TIMESTAMP
        - number formats: INT64, FLOAT64, NUMERIC
        - misc formats: STRING, BOOL, ARRAY, RECORD
    From check_value_type in utils.py
    :param value: value on which to perform data type analysis
    :return: data type in BigQuery Standard SQL format
    """
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if value != value:  # NaN case
        return "FLOAT64"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "RECORD"
    if not value:
        return None

    # A sequence of numbers starting with a 0 represents a string id,
    # but int() check will pass and data loss would occur.
    if value.startswith("0") and len(value) > 1 and ':' not in value and '-' not in value and '.' not in value:
        return "STRING"

    # check to see if value is numeric, float or int;
    # differentiates between these types and datetime or ids, which may be composed of only numbers or symbols
    if '.' in value and ':' not in value and "E+" not in value and "E-" not in value:
        try:
            int(value)
            return "INT64"
        except ValueError:
            try:
                float(value)
                decimal_val = int(value.split('.')[1])

                # if digits right of decimal place are all zero, float can safely be cast as an int
                if not decimal_val:
                    return "INT64"
                return "FLOAT64"
            except ValueError:
                return "STRING"

    # numeric values are numbers with special encoding, like an exponent or sqrt symbol
    elif value.isnumeric() and not value.isdigit() and not value.isdecimal():
        return "NUMERIC"

    """
    BIGQUERY'S CANONICAL DATE/TIME FORMATS:
    (see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    """

    # Check for BigQuery DATE format: 'YYYY-[M]M-[D]D'
    date_re_str = r"[0-9]{4}-(0[1-9]|1[0-2]|[0-9])-(0[1-9]|[1-2][0-9]|[3][0-1]|[1-9])"
    date_pattern = re.compile(date_re_str)
    if re.fullmatch(date_pattern, value):
        return "DATE"

    # Check for BigQuery TIME format: [H]H:[M]M:[S]S[.DDDDDD]
    time_re_str = r"([0-1][0-9]|[2][0-3]|[0-9]{1}):([0-5][0-9]|[0-9]{1}):([0-5][0-9]|[0-9]{1}])(\.[0-9]{1,6}|)"
    time_pattern = re.compile(time_re_str)
    if re.fullmatch(time_pattern, value):
        return "TIME"

    # Check for BigQuery TIMESTAMP format: YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
    timestamp_re_str = date_re_str + r'( |T)' + time_re_str + r"([ \-:A-Za-z0-9]*)"
    timestamp_pattern = re.compile(timestamp_re_str)
    if re.fullmatch(timestamp_pattern, value):
        return "TIMESTAMP"

    try:
        util.strtobool(value)
        return "BOOL"
    except ValueError:
        pass

    # Final check for int and float values. This will catch a simple integers
    # or edge case float values, like infinity, scientific notation, etc.
    try:
        int(value)
        return "INT64"
    except ValueError:
        try:
            float(value)
            return "FLOAT64"
        except ValueError:
            return "STRING"


def resolve_type_conflict(field, types_set):
    """
    Resolve BigQuery column data type precedence, where multiple types are detected. Rules for type conversion based on
    BigQuery's implicit conversion behavior.
    See https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#coercion
    From resolve_type_conflict in utils.py
    :param types_set: Set of BigQuery data types in string format
    :param field: field name
    :return: BigQuery data type with the highest precedence
    """

    datetime_types = {"TIMESTAMP", "DATE", "TIME"}
    number_types = {"INT64", "FLOAT64", "NUMERIC"}

    # fix to make even proper INT64 ids into STRING ids
    if "_id" in field:
        return "STRING"

    if len(types_set) == 0:
        # fields with no type values default to string--this would still be safe for skip-row analysis of a data file
        return "STRING"
    if len(types_set) == 1:
        # only one data type for field, return it
        return list(types_set)[0]

    # From here, the field's types_set contains at least two values; select based on BigQuery's implicit conversion
    # rules; when in doubt, declare a string to avoid risk of data loss

    if "ARRAY" in types_set or "RECORD" in types_set:
        # these types cannot be implicitly converted to any other, exit
        util_logger.error(f"Invalid datatype combination for {field}: {types_set}")
        util_logger.error("", TypeError)
        sys.exit()

    if "STRING" in types_set:
        # if it's partly classified as a string, it has to be a string--other data type values are converted
        return "STRING"

    if len(types_set) == 2 and "INT64" in types_set and "BOOL" in types_set:
        # 1 or 0 are labelled bool by type checker; any other ints are labelled as ints.
        # If both 1 and 0 occur, AND there are traditional Boolean values in the column, then it'll be declared a BOOL;
        # otherwise, it should be INT64
        return "INT64"

    has_datetime_type = False

    # are any of the data types datetime types -- {"TIMESTAMP", "DATE", "TIME"}?
    for datetime_type in datetime_types:
        if datetime_type in types_set:
            has_datetime_type = True
            break

    has_number_type = False

    # are any of the data types number types -- {"INT64", "FLOAT64", "NUMERIC"}?
    for number_type in number_types:
        if number_type in types_set:
            has_number_type = True
            break

    # What, data source?! Okay, fine, be a string
    if has_datetime_type and has_number_type:
        # another weird edge case that really shouldn't happen
        return "STRING"

    # Implicitly convert to inclusive datetime format
    if has_datetime_type:
        if "TIME" in types_set:
            # TIME cannot be implicitly converted to DATETIME
            return "STRING"
        # DATE and TIMESTAMP *can* be implicitly converted to DATETIME
        return "DATETIME"

    # Implicitly convert to inclusive number format
    if has_number_type:
        # only number types remain
        # INT64 and NUMERIC can be implicitly converted to FLOAT64
        # INT64 can be implicitly converted to NUMERIC
        if "FLOAT64" in types_set:
            return "FLOAT64"
        elif "NUMERIC" in types_set:
            return "NUMERIC"

    # No BOOL, DATETIME combinations allowed, or whatever other randomness occurs--return STRING
    return "STRING"


def find_types(file, sample_interval):
    """
    Finds the field type for each column in the file
    From find_types in support.py
    :param file: file name
    :type file: basestring
    :param sample_interval:sampling interval, used to skip rows in large datasets; defaults to checking every row
        example: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return: a tuple with a list of [field, field type]
    :rtype: tuple ([field, field_type])
    """
    column_list = get_column_list_tsv(tsv_fp=file, header_row_index=0)
    field_types = aggregate_column_data_types_tsv(file, column_list,
                                                  sample_interval=sample_interval,
                                                  skip_rows=1)
    final_field_types = resolve_type_conflicts(field_types)
    typing_tups = []
    for column in column_list:
        tup = (column.rstrip("\n"), final_field_types[column])
        typing_tups.append(tup)

    return typing_tups


def resolve_type_conflicts(types_dict):
    """
    Iteratively resolve data type conflicts for non-nested type dicts (e.g. if there is more than one data type found,
    select the superseding type.)
    Function from resolve_type_conflicts in utils.py
    :param types_dict: dict containing columns and all detected data types
    :type types_dict: dict {str: set}
    :return dict containing the column name and its BigQuery data type.
    :rtype dict of {str: str}
    """
    type_dict = dict()

    for field, types_set in types_dict.items():
        type_dict[field] = resolve_type_conflict(field, types_set)

    return type_dict


def create_schema_hold_list(typing_tups, field_schema, holding_list, static=True):
    """
    Create a file of schema
    From create_schema_hold_list in support.py
    :param typing_tups: dictionary of data types for each field
    :param field_schema: File of field schema
    :param holding_list: File for final schema
    :param static: Whether to use the dynmaic typing from typing_tups or static from field_schema
    :return: Whether the function worked
    """
    with open(field_schema, mode='r') as field_schema_file:
        all_field_schema = json_loads(field_schema_file.read())

    typed_schema = []
    for tup in typing_tups:
        util_logger.info(tup)
        field_dict = all_field_schema[tup[0].rstrip("\n")]
        if tup[1] is None or tup[1][0:4] != field_dict["type"][0:4]:
            util_logger.warning(f"{tup[0]} types do not match.")
            util_logger.warning(f"Dynamic type ({tup[1]}) does not equal static type ({field_dict['type']})")

        if field_dict["exception"] == "":
            if static:
                util_logger.info(f"\tsetting type to static type {field_dict['type']}")
                tup = (tup[0], field_dict["type"])
            else:
                util_logger.info(f"\tsetting type to dynamic type ({tup[1]})")

        if field_dict["description"]:
            full_field_dict = {
                "name": tup[0],
                "type": tup[1],
                "description": field_dict["description"]
            }
            typed_schema.append(full_field_dict)
        else:
            util_logger.warning(f"{tup[0]} field description needs to be updated separately.")
            no_desc = {
                "name": tup[0],
                "type": tup[1],
                "description": "No description"
            }
            typed_schema.append(no_desc)

    with open(holding_list, mode='w') as schema_hold_list:
        schema_hold_list.write(json_dumps(typed_schema))

    return True


def update_schema_tags(program_mappings, release=None, release_date=None, release_anchor=None, program=None):
    """
    Update a BQ table labels
    Function from update_schema_tags in support.py
    :param program_mappings:
    :param release:
    :param release_date:
    :param release_anchor:
    :param program:
    :return:
    """

    schema = dict()

    if release:
        schema['---tag-release---'] = release
        schema['---tag-release-url-anchor---'] = str(release_anchor)

    if release_date:
        schema['---tag-extracted-month-year---'] = release_date

    if program is not None:
        schema['---tag-program---'] = program
        if 'program_label' in program_mappings[program].keys():
            schema['---tag-program-name-lower---'] = program_mappings[program]['program_label']
        else:
            schema['---tag-program-name-lower---'] = None

        if 'program_label_0' in program_mappings[program].keys():
            schema['---tag-program-name-lower-0---'] = program_mappings[program]['program_label_0']
        else:
            schema['---tag-program-name-lower-0---'] = None

        if 'program_label_1' in program_mappings[program].keys():
            schema['---tag-program-name-lower-1---'] = program_mappings[program]['program_label_1']
        else:
            schema['---tag-program-name-lower-1---'] = None

    return schema


def install_table_field_desc(table_id, new_descriptions):
    """
    Modify an existing table's field descriptions
    Function adapted from update_schema in utils.py and support.py
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    new_schema = []

    for schema_field in table.schema:
        field = schema_field.to_api_repr()
        name = field['name']

        if name in new_descriptions.keys() and new_descriptions[name]['exception'] == '':
            field['description'] = new_descriptions[name]['description']
        elif name in new_descriptions.keys() and new_descriptions[name]['exception'] is not None:
            print(f"Field {name} has an exception listed: {new_descriptions[name]['exception']}")
        else:
            print(f"{name} field is not listed in json")

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def update_status_tag(target_dataset, dest_table, status, project=None):
    """
    Update the status tag of a big query table once a new version of the table has been created.
    Function from update_status_tag in support.py
    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param status: The value you want to change the table label to
    :type status: basestring
    :param project: Project name
    :type project: basestring
    :return: Whether the function works
    :rtype: bool
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    table = client.get_table(table_ref)
    table.labels = {"status": status}
    table = client.update_table(table, ["labels"])
    return True


def write_table_schema_with_generic(table_id, schema_tags=None, metadata_fp=None,
                                    field_desc_fp=None):
    """
    Create table metadata schema using generic schema files in BQEcosystem and schema tags defined in yaml config files.
    Function from write_table_schema_with_generic in support.py
    :param table_id: Table id for which metadata will be added
    :param schema_tags: dict of tags to substitute into generic schema file (used for customization)
    :param metadata_fp: File path for the metadata
    :param field_desc_fp: File path for the field description file
    :return: If the function worked
    """

    if metadata_fp is not None:
        write_table_metadata_with_generic(metadata_fp, table_id, schema_tags)

    if field_desc_fp is not None:
        with open(field_desc_fp, mode='r') as field_desc:
            field_desc_dict = json_loads(field_desc.read())
        install_table_field_desc(table_id, field_desc_dict)

    return True


def write_table_metadata_with_generic(metadata_fp, table_id, schema_tags):
    """
    Updates the tags in the generic schema file then writes the schema to the table metadata in BigQuery.
    This function is an adaption of the add_generic_table_metadata function in utils.py
    :param metadata_fp: File path for the metadata
    :param table_id: Table id for which metadata will be added
    :param schema_tags: dict of tags to substitute into generic schema file
    """
    final_table_metadata = {}

    with open(metadata_fp, mode='r') as metadata_dict_file:
        metadata_dict = json_loads(metadata_dict_file.read())

        for main_key, main_value in metadata_dict.items():
            if type(main_value) is dict:
                final_table_metadata[main_key] = {}
                for sub_key, sub_value in metadata_dict[main_key].items():
                    if sub_value[1:4] == "---":
                        if schema_tags[sub_value.strip("{}")]:
                            final_table_metadata[main_key][sub_key] = sub_value.format(**schema_tags)
                        else:
                            util_logger.info(f"{sub_key} skipped")
                    else:
                        util_logger.info("no tags")
                        final_table_metadata[main_key][sub_key] = sub_value
            else:
                final_table_metadata[main_key] = main_value.format(**schema_tags)

    install_table_metadata(table_id, final_table_metadata)


def install_table_metadata(table_id, metadata):
    """
    Modify an existing BigQuery table's metadata (labels, friendly name, description) using metadata dict argument
    Function was adapted from update_table_metadata function in utils.py
    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']
    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']

