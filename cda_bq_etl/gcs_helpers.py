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
import os
from typing import Union, Optional

from google.cloud import storage, exceptions
from cda_bq_etl.utils import get_scratch_fp, has_fatal_error

Params = dict[str, Union[str, dict, int]]


def download_from_bucket(params: Params,
                         filename: str,
                         bucket_path: Optional[str] = None,
                         dir_path: Optional[str] = None):
    """
    Download file from Google storage bucket onto VM.
    :param params: params from yaml config, used to retrieve default bucket directory path
    :param filename: Name of file to download
    :param bucket_path: Optional, override default bucket directory path
    :param dir_path: Optional, location in which to download file;
                     if not specified, defaults to scratch folder defined in params
    """
    if not dir_path:
        file_path = get_scratch_fp(params, filename)
    else:
        file_path = f"{dir_path}/{filename}"

    if os.path.isfile(file_path):
        os.remove(file_path)

    storage_client = storage.Client(project="")
    if bucket_path:
        blob_name = f"{bucket_path}/{filename}"
    else:
        blob_name = f"{params['WORKING_BUCKET_DIR']}/{filename}"
    bucket = storage_client.bucket(params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    with open(file_path, 'wb') as file_obj:
        blob.download_to_file(file_obj)

    if os.path.isfile(file_path):
        print(f"File successfully downloaded from bucket to {file_path}")


def upload_to_bucket(params: Params, scratch_fp: str, delete_local: bool = False, verbose: bool = True):
    """
    Upload file to a Google storage bucket (bucket/directory location specified in YAML config).
    :param params: bq param object from yaml config
    :param scratch_fp: name of file to upload to bucket
    :param delete_local: delete scratch file created on VM
    :param verbose: if True, print a confirmation for each file uploaded
    """
    if not os.path.exists(scratch_fp):
        has_fatal_error(f"Invalid filepath: {scratch_fp}", FileNotFoundError)

    try:
        storage_client = storage.Client(project="")

        jsonl_output_file = scratch_fp.split('/')[-1]
        bucket_name = params['WORKING_BUCKET']
        bucket = storage_client.bucket(bucket_name)

        blob_name = f"{params['WORKING_BUCKET_DIR']}/{jsonl_output_file}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(scratch_fp)

        if verbose:
            print(f"Successfully uploaded file to {bucket_name}/{blob_name}. ", end="")

        if delete_local:
            os.remove(scratch_fp)
            if verbose:
                print("Local file deleted.")
        else:
            if verbose:
                print(f"Local file not deleted.")

    except exceptions.GoogleCloudError as err:
        has_fatal_error(f"Failed to upload to bucket.\n{err}")
    except FileNotFoundError as err:
        has_fatal_error(f"File not found, failed to access local file.\n{err}")
