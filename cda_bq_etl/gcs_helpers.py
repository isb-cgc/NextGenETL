# Copyright 2023-2025, Institute for Systems Biology

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
# Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Google Cloud Storage helper functions."""

import logging
import os
import sys
from typing import Optional

from google.cloud import storage, exceptions
from cda_bq_etl.utils import get_scratch_fp, get_filepath
from cda_bq_etl.custom_typing import Params
from google.cloud import secretmanager
import google_crc32c
import boto3


def download_from_external_bucket(uri_path: str,
                                  dir_path: str,
                                  filename: str,
                                  project: Optional[str] = None,
                                  expand_fp: bool = True):
    """
    Download file from Google storage bucket onto VM.

    :param uri_path: GCS uri path
    :type uri_path: str
    :param dir_path: VM location for downloaded file
    :type dir_path: str
    :param filename: Name of file to download
    :type filename: str
    :param project: GCS project from which to download blob
    :type project: Optional[str]
    :param expand_fp: If True, expand filepath; else use relative path {dir_path}/{filename}
    :type expand_fp: bool
    """
    if expand_fp:
        file_path = get_filepath(dir_path, filename)
    else:
        file_path = f"{dir_path}/{filename}"

    if os.path.isfile(file_path):
        os.remove(file_path)

    if project:
        storage_client = storage.Client(project=project)
    else:
        storage_client = storage.Client()

    with open(file_path, 'wb') as file_obj:
        uri = f"{uri_path}/{filename}"
        storage_client.download_blob_to_file(blob_or_uri=uri, file_obj=file_obj)

    logger = logging.getLogger('base_script.cda_bq_etl.gcs_helpers')

    if os.path.isfile(file_path):
        logger.info(f"File successfully downloaded from bucket to {file_path}")
    else:
        logger.error(f"Download failed for {uri_path}.")


def download_from_bucket(params: Params,
                         filename: str,
                         bucket_path: Optional[str] = None,
                         dir_path: Optional[str] = None,
                         project: str = ""):
    """
    Download file from Google storage bucket onto VM.

    :param params: params from yaml config, used to retrieve default bucket directory path
    :type params: Params
    :param filename: Name of file to download
    :type filename: str
    :param bucket_path: Optional, override default bucket directory path
    :type bucket_path: Optional[str]
    :param dir_path: Optional, location in which to download file;
                     if not specified, defaults to scratch folder defined in params
    :type dir_path: Optional[str]
    :param project: Optional, defined if project outside the default scope; defaults to empty string
    :type project: str
    """
    if not dir_path:
        file_path = get_scratch_fp(params, filename)
    else:
        file_path = f"{dir_path}/{filename}"

    if os.path.isfile(file_path):
        os.remove(file_path)

    storage_client = storage.Client(project=project)

    if bucket_path:
        blob_name = f"{bucket_path}/{filename}"
    else:
        blob_name = f"{params['WORKING_BUCKET_DIR']}/{filename}"
    bucket = storage_client.bucket(params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    with open(file_path, 'wb') as file_obj:
        blob.download_to_file(file_obj)

    if os.path.isfile(file_path):
        logger = logging.getLogger('base_script.cda_bq_etl.gcs_helpers')
        logger.info(f"File successfully downloaded from bucket to {file_path}")


def upload_to_bucket(params: Params, scratch_fp: str, delete_local: bool = False, verbose: bool = True):
    """
    Upload file to a Google storage bucket (bucket/directory location specified in YAML config).

    :param params: bq param object from yaml config
    :type params: Params
    :param scratch_fp: name of file to upload to bucket
    :type scratch_fp: str
    :param delete_local: delete scratch file created on VM
    :type delete_local: bool
    :param verbose: if True, log a confirmation for each file uploaded
    :type verbose: bool
    """
    logger = logging.getLogger('base_script.cda_bq_etl.gcs_helpers')

    if not os.path.exists(scratch_fp):
        logger.critical(f"Invalid filepath: {scratch_fp}", FileNotFoundError)
        sys.exit(-1)

    try:
        storage_client = storage.Client(project="")

        output_file = scratch_fp.split('/')[-1]
        bucket_name = params['WORKING_BUCKET']
        bucket = storage_client.bucket(bucket_name)

        blob_name = f"{params['WORKING_BUCKET_DIR']}/{output_file}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(scratch_fp)

        if verbose:
            logger.info(f"Successfully uploaded file to {bucket_name}/{blob_name}.")

        if delete_local:
            os.remove(scratch_fp)
            if verbose:
                logger.info("Local file deleted.")
        else:
            if verbose:
                logger.info(f"Local file not deleted.")

    except exceptions.GoogleCloudError as err:
        logger.critical(f"Failed to upload to bucket.\n{err}")
        sys.exit(err)
    except FileNotFoundError as err:
        logger.critical(f"File not found, failed to access local file.\n{err}")
        sys.exit(-1)


def transfer_between_buckets(params: Params,
                             source_bucket_name: str,
                             bucket_file: str,
                             target_bucket_name: str,
                             target_bucket_file: Optional[str] = None):
    """
    Transfer file from source bucket to target bucket.

    :param params: params from YAML config
    :type params: Params
    :param source_bucket_name: Name of source bucket
    :type source_bucket_name: str
    :param bucket_file: Name of src file
    :type bucket_file: str
    :param target_bucket_name: Name of target bucket
    :type target_bucket_name: str
    :param target_bucket_file: Name of target file, defaults to source_bucket_name (if set to None)
    :type target_bucket_file: Optional[str]
    """
    logger = logging.getLogger('base_script.cda_bq_etl.gcs_helpers')

    try:
        storage_client = storage.Client(project="")

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(bucket_file)

        destination_bucket = storage_client.bucket(target_bucket_name)

        if target_bucket_file is None:
            target_bucket_file = bucket_file

        destination_blob = f"{params['WORKING_BUCKET_DIR']}/{target_bucket_file}"

        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob)

    except exceptions.GoogleCloudError as err:
        logger.critical(f"Failed to upload to bucket.\n{err}")
        sys.exit(err)

#
# Functions to support use of Secret Manager for holding AWS credentials
#

def pull_check_and_decode(client, name):

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return None

    # Return the secret payload.

    payload = response.payload.data.decode("UTF-8")

    return payload

def pull_secret_version(client, project_id, secret_id, version_id):

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    retval = pull_check_and_decode(client, name)
    return retval

# Google VM Utils #

def pull_from_buckets_aws(pull_list, local_files_dir, project_id=None, secret_id=None):
    """
    Adapted from GCS version above.
    """
    num_files = len(pull_list)
    print(f"Begin {num_files} bucket copies...")  # todo make logger
    # Create the Secret Manager client.
    if secret_id is not None:
        client = secretmanager.SecretManagerServiceClient()
        access_key = pull_secret_version(client, project_id, secret_id["id"], "latest")
        secret_key = pull_secret_version(client, project_id, secret_id["key"], "latest")
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    else:
        s3 = boto3.client('s3')
    copy_count, output_list = 0, []
    # for url in pull_list[:3]:
    for url in pull_list:
        print(url)
        path_pieces = up.urlparse(url)
        print(path_pieces)
        dir_name = os.path.dirname(path_pieces.path)
        make_dir = f"{local_files_dir}{dir_name}"
        print(make_dir)
        # os.makedirs(makedir, exists_ok=True)
        full_file = f"{local_files_dir}{path_pieces.path}"
        print(full_file)
        # s3.download_file(path_pieces.netloc, path_pieces.path.lstrip('/'), full_file)
        priunt(path_pieces.netloc, path_pieces.path.lstrip('/'), full_file)
        output_list.append((url, full_file))
        copy_count += 1
        if (copy_count % 10) == 0:
            print_progress_bar(copy_count, num_files)
    print_progress_bar(num_files, num_files)
