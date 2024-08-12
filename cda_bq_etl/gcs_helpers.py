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
import os
import sys
from typing import Union, Optional

from google.cloud import storage, exceptions
from cda_bq_etl.utils import get_scratch_fp, get_filepath

Params = dict[str, Union[str, dict, int]]


def download_from_external_bucket(uri_path: str, dir_path: str, filename: str, project: str = None,
                                  expand_fp: bool = True):

    """
    Download file from Google storage bucket onto VM.
    :param project: GCS project from which to download blob
    :param uri_path: GCS uri path
    :param filename: Name of file to download
    :param dir_path: VM location for downloaded file
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
                         project: Optional[str] = ""):
    """
    Download file from Google storage bucket onto VM.
    :param params: params from yaml config, used to retrieve default bucket directory path
    :param filename: Name of file to download
    :param bucket_path: Optional, override default bucket directory path
    :param dir_path: Optional, location in which to download file;
                     if not specified, defaults to scratch folder defined in params
    :param project: Optional, defined if project outside the default scope
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
    :param scratch_fp: name of file to upload to bucket
    :param delete_local: delete scratch file created on VM
    :param verbose: if True, log a confirmation for each file uploaded
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
        sys.exit(err)


def transfer_between_buckets(source_bucket_name, bucket_file, target_bucket_name, target_bucket_file=None):
    """
    todo
    :param source_bucket_name:
    :param bucket_file:
    :param target_bucket_name:
    :param target_bucket_file:
    :return:
    """
    logger = logging.getLogger('base_script.cda_bq_etl.gcs_helpers')

    try:
        storage_client = storage.Client(project="")

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(bucket_file)

        destination_bucket = storage_client.bucket(target_bucket_name)

        if target_bucket_file is None:
            target_bucket_file = bucket_file

        source_bucket.copy_blob(source_blob, destination_bucket, target_bucket_file)

    except exceptions.GoogleCloudError as err:
        logger.critical(f"Failed to upload to bucket.\n{err}")
        sys.exit(err)
