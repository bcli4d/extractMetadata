# -*- coding: utf-8 -*-
"""
Walk a directory tree starting at the --dir paramenter. Expand each zip
file and Build a TSV table of tag from all extracted dcm files. Specific
tags to collect are listed in tags file
Output result to standard out.
"""
from __future__ import print_function

import os, sys
import argparse
import zipfile
from os.path import join
import time
import json
import subprocess
import shutil
import httplib2
from oauth2client.client import GoogleCredentials
import distutils

from googleapiclient import discovery
from google.oauth2 import service_account

from google.auth.transport import requests
from googleapiclient.errors import HttpError

from google.cloud import bigquery

from email import encoders
#from email.mime.application import MIMEApplication
import email.mime.application as application
#from email.mime.multipart import MIMEMultipart
import email.mime.multipart as multipart
import email

import numpy as np
import pandas as pd

zipFileCount = 0
zips = set()
#includes = []
#series = []
dones = []
service_account_json = ""
api_key = ""


HEALTHCARE_API_URL = 'https://healthcare.googleapis.com/v1alpha'


# Get the series we've already processed
def loadDones(args):
    if os.path.exists(args.dones):
        with open(args.dones) as f:
            strings = f.read().splitlines()
            dones = set(strings)
            #print("dones {}".format(dones))
    else:
        dones = set()
    return dones

    # Add name of processed file
def appendDones(args, zip):
    with open(args.dones, 'a') as f:
        f.write("{}\n".format(zip))

'''
# Build a list of column names to include
def loadIncludes(args):
    with open(args.includes) as f:
        strings = f.read().splitlines()
        includes = list(strings)
        #print("includes: {}".format(includes))
    return includes

# Save possibly updated list of column names to include
def saveIncludes(args):
    with open(args.includes, 'w') as f:
        f.write(json.dumps(includes).encode())
'''

# Build a list of files to process
def loadZips(args):
    with open(args.zips) as f:
        strings = f.read().splitlines()
        zips = sorted(list(strings))
        #print("zips: {}".format(zips))
    return zips

def get_client(args, api_key):
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials JSON."""
    api_scopes = ['https://www.googleapis.com/auth/cloud-platform']
    api_version = 'v1alpha'
    discovery_api = 'https://healthcare.googleapis.com/$discovery/rest'
    service_name = 'healthcare'

    credentials = service_account.Credentials.from_service_account_file(
        args.service_account)
    scoped_credentials = credentials.with_scopes(api_scopes)

    discovery_url = '{}?labels=CHC_ALPHA&version={}&key={}'.format(
        discovery_api, api_version, api_key)

    thing = discovery.build(
        service_name,
        api_version,
        discoveryServiceUrl=discovery_url,
        credentials=scoped_credentials)

    return thing

def get_dataset(args,
        api_key):
    """Gets any metadata associated with a dataset."""
    client = get_client(args, api_key)
    dataset_name = 'projects/{}/locations/{}/datasets/{}'.format(
        args.project_id, args.location, args.gh_dataset_id)

    datasets = client.projects().locations().datasets()
    dataset = datasets.get(name=dataset_name).execute()

    print('Name: {}'.format(dataset.get('name')))
    print('Time zone: {}'.format(dataset.get('timeZone')))

    return dataset

def create_gh_dataset(args,
        api_key):
    """Creates a dataset."""
    dataset = get_dataset(args, api_key)
    if dataset['name'].find(args.gh_dataset_id) < 0 :
        client = get_client(args, api_key)
        dataset_parent = 'projects/{}/locations/{}'.format(
            args.project_id, args.location)

        body = {}

        request = client.projects().locations().datasets().create(
            parent=dataset_parent, body=body, datasetId=args.gh_dataset_id)

        try:
            response = request.execute()
            print('Created dataset: {}'.format(gh_dataset_id))
            return response
        except HttpError as e:
            print('Error, dataset not created: {}'.format(e))
            return ""

def list_dicom_stores(args, api_key):
    """Lists the DICOM stores in the given dataset."""
    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(
        args.project_id, args.location, args.gh_dataset_id)
    dicom_store_name = '{}/dicomStores/{}'.format(
        dicom_store_parent, args.gh_dicom_store_id)

#    request = client.projects().locations().datasets().dicomStores().execute(name=dicom_store_name)

    dicom_stores = client.projects().locations().datasets().dicomStores().list(
        parent=dicom_store_parent).execute().get('dicomStores', [])

    for dicom_store in dicom_stores:
        print('DICOM store: {}\n'
              'Notification config: {}'.format(
                  dicom_store.get('name'),
                  dicom_store.get('notificationConfig'),
              ))

    return dicom_stores



def delete_dicom_store(args, api_key):
    """Deletes the specified DICOM store."""

    dicom_stores = list_dicom_stores(args, api_key)
    if len(dicom_stores) > 0:
        for store in dicom_stores:
            if store['name'].find(args.gh_dicom_store_id) >= 0:
                client = get_client(args, api_key)
                dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(
                    args.project_id, args.location, args.gh_dataset_id)
                dicom_store_name = '{}/dicomStores/{}'.format(
                    dicom_store_parent, args.gh_dicom_store_id)

                request = client.projects().locations().datasets().dicomStores().delete(name=dicom_store_name)

                try:
                    response = request.execute()
                    print('Deleted DICOM store: {}'.format(args.gh_dicom_store_id))
                    return response
                except HttpError as e:
                    print('Error, DICOM store not deleted: {}'.format(e))
                    return ""


def create_dicom_store(args, api_key, delete=False):
    """Creates a new DICOM store within the parent dataset."""

    if delete:
        delete_dicom_store(args, api_key)

    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(args.project_id, args.location, args.gh_dataset_id)

    body = {}

    request = client.projects().locations().datasets().dicomStores().create(
        parent=dicom_store_parent, body=body, dicomStoreId=args.gh_dicom_store_id)

    try:
        response = request.execute()
        print('Created DICOM store: {}'.format(args.gh_dicom_store_id))
        return response
    except HttpError as e:
        print('Error, DICOM store not created: {}'.format(e))
        return ""


# Copy a zip file for some series from GCA and extract dicoms
def getZipFromGCS(args, zip):
    zipfileName = join(args.scratch,'dcm.zip')
    dicomDirectory = join(args.scratch,'dicoms')

    subprocess.call(['gsutil', 'cp', zip, zipfileName])

    # Open the file and extract the .dcm files to scratch directory
    zf = zipfile.ZipFile(zipfileName)
    zf.extractall(dicomDirectory)

    return dicomDirectory

# Remove zip file and extracted .dcms of a series after processing
def cleanupSeries(args, api_key):

    zipfileName = join(args.scratch,'dcm.zip')
    dicomDirectory = join(args.scratch,'dicoms')
    shutil.rmtree(dicomDirectory)
    os.remove(zipfileName)

def wait_for_operation_completion(args, api_key, path, timeout, start_time):

    """Export metadata to a BQ table"""
    client = get_client(args, api_key)

    request = client.projects().locations().datasets().operations().get(name=path)

    success = False
    while time.time() <timeout:
        elapsed = int((time.time() - start_time))
        if elapsed % 10 == 0:
            print('Waiting for operation completion for {} seconds'.format(elapsed))
        response = request.execute()
        if 'done' in response:
            if response['done'] == True and 'error' not in response:
                success = True;
            break
        time.sleep(1)

    print('Full response:\n{0}'.format(response))
    assert success, "operation did not complete successfully in time limit"
    print('Success!')
    return response


def export_dicom_metadata(args, api_key):

    """Export metadata to a BQ table"""
    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(
        args.project_id, args.location, args.gh_dataset_id)
    dicom_store_name = '{}/dicomStores/{}'.format(
        dicom_store_parent, args.gh_dicom_store_id)



    if not args.incremental and not args.compact:
        dest_table = args.bq_cum_table
        body = {
          'outputConfig': {
            'bigQueryDestination': {
                'dataset': args.bq_dataset_id,
                'table': dest_table,
                'overwriteTable': 'True'
            }
          }
        }
    else:
        dest_table = args.bq_temp_table
        body = {
            'outputConfig': {
                'bigQueryDestination': {
                    'dataset': args.bq_dataset_id,
                    'table': dest_table,
                    'overwriteTable': 'True'
                }
            }
        }

    request = client.projects().locations().datasets().dicomStores().export(
        name=dicom_store_name, body=body)

    try:
        response = request.execute()
        print('Exporting DICOM metadata to table {}.{}: '.format(args.bq_dataset_id, dest_table))
        metadata_operation_name = response['name']

        start_time = time.time()
        timeout = start_time + 10 * 60  # Wait up to 10 minutes.
        path = join(HEALTHCARE_API_URL, metadata_operation_name)
        path = metadata_operation_name
        response = wait_for_operation_completion(args, api_key, path, timeout, start_time)

        return response
    except HttpError as e:
        print('Error, DICOM metadata not exported: {}'.format(e))
        return ""

def get_session(service_account_json):
    """Returns an authorized Requests Session class using the service account
    credentials JSON. This class is used to perform requests to the
    Healthcare API endpoint."""

    # Pass in the credentials and project ID. If none supplied, get them
    # from the environment.
    credentials = service_account.Credentials.from_service_account_file(
        service_account_json)
    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])

    # Create a requests Session object with the credentials.
    session = requests.AuthorizedSession(scoped_credentials)

    return session

# Get a list of the SeriesInstanceUID already in the datastore
def dicomweb_search_series(args):
    """Handles the GET requests specified in DICOMweb standard."""
    global series
    url = '{}/projects/{}/locations/{}'.format(args.base_url,
                                               args.project_id, args.location)

    # Make an authenticated API request
    session = get_session(args.service_account)

    headers = {
        'Content-Type': 'application/dicom+json; charset=utf-8'
    }

    skip = 0
    limit = 1000
    series_list = []
    while True:

        dicomweb_path = '{}/datasets/{}/dicomStores/{}/dicomWeb/series?limit={}'.format(
            url, args.gh_dataset_id, args.gh_dicom_store_id, limit)


        response = session.get(dicomweb_path, headers=headers)
        response.raise_for_status()

        batch = response.json()
        if len(batch) == 0:
            break
        series_list.extend(batch)
        skip += limit

    for element in series_list:
        series.append(element['0020000E']['Value'][0])
        if args.verbosity > 1:
            print('{} in datastore'.format(element['0020000E']['Value'][0]))

    return series


# Store a DICOM instance in the datastore
def dicomweb_store_instance(args, dcm_file):

    """ Handles the POST requests specified in the DICOMweb standard. """

    url = '{}/projects/{}/locations/{}'.format(args.base_url,
                                               args.project_id, args.location)

    dicomweb_path = '{}/datasets/{}/dicomStores/{}/dicomWeb/studies'.format(
        url, args.gh_dataset_id, args.gh_dicom_store_id)

    # Make an authenticated API request
    session = get_session(args.service_account)

    with open(dcm_file) as dcm:
        dcm_content = dcm.read()

    # All requests to store an instance are multipart messages, as designated
    # by the multipart/related portion of the Content-Type. This means that
    # the request is made up of multiple sets of data that are combined after
    # the request completes. Each of these sets of data must be separated using
    # a boundary, as designated by the boundary portion of the Content-Type.
    multipart_body = multipart.MIMEMultipart(
        subtype='related', boundary=email.generator._make_boundary())
    part = application.MIMEApplication(
        dcm_content, 'dicom', _encoder=encoders.encode_noop)
    multipart_body.attach(part)
    boundary = multipart_body.get_boundary()

    content_type = ('multipart/related; type="application/dicom"; ' + 'boundary="%s"') % boundary
    headers = {'Content-Type': content_type}

    try:
        response = session.post(
            dicomweb_path,
            data=multipart_body.as_string(),
            headers=headers)
        response.raise_for_status()
        if args.verbosity > 2:
            print('Stored DICOM instance:')
            print(response.text)
        return response
    except HttpError as err:
        print(err)
        return ""


def load_series_into_dicom_store(args, zip, api_key):
#    create_dicom_store(args, api_key)
    zipFilesPath = getZipFromGCS(args, zip)

    dicoms = os.listdir(zipFilesPath)
    dicoms.sort()

    for dicom in dicoms:
        dicomweb_store_instance(args, join(zipFilesPath,dicom))
#    export_dicom_metadata(args, api_key)

    appendDones(args, zip)
    cleanupSeries(args, api_key)


# Append per-series metadata to a table where it is being accumulated
def append_to_cumulative_table(args, api_key):
    global excludes
    client = bigquery.Client(project=args.project_id)

    dataset_ref = client.dataset(args.bq_dataset_id)
    table_ref = dataset_ref.table(args.bq_temp_table)
    t1_meta = client.get_table(table_ref)  # API Request

    if args.verbosity > 3:
        # View table properties
        print(t1_meta.schema)
        print(t1_meta.description)
        print(t1_meta.num_rows)

    if args.compact:
        # This path finds all columns that have a single value and creates a single row to be
        # appended to the cumulative table.

        # We do not include structured data in this path so
        # find the column names that are not repeated or nested
        # and are not vendor specific
        colList = []
        for mi in t1_meta.schema:
            if mi.mode != 'REPEATED' and mi.field_type != 'RECORD':  # struct too?
                l1 = ([mi.name, mi.mode, mi.field_type])
                colList.append(l1)

        if args.verbosity > 2:
            print(colList[0:5])

        # Then for columns that are not repeated or nested, get the number of unique values #
        sqlstr = 'SELECT\n'

        n = len(colList)
        print(n)

        for li in colList[0:(n - 1)]:
            sqlstr += 'COUNT(DISTINCT( `' + li[0] + '` )),\n'
        sqlstr += 'COUNT(DISTINCT( `' + colList[(n - 1)][0] + '` ))\n'
        sqlstr += 'FROM `{}.{}.{}`\n'.format(args.project_id, args.bq_dataset_id, args.bq_temp_table)

        if args.verbosity > 2:
            print(sqlstr)

        query_job = client.query(sqlstr, )  # API request - starts the query
        query_job.done()
        df1 = query_job.to_dataframe()

        # Here's the columns that have a single value.
        if args.verbosity > 2:
            for i, ci in enumerate(df1.columns):
                uvals = int(df1[ci])
                if uvals == 1:
                    print(str(uvals) + '\t' + colList[i][0])

        # We use the results to create a new query
        # and get the single row identifier for this dataset.table
        sqlstr = 'SELECT\n'

        for i, ci in enumerate(df1.columns[0:len(df1.columns)-1]):
            if int(df1[ci]) == 1:
                sqlstr += '`{}`,\n'.format( colList[i][0])
        if int(df1[df1.columns[-1]]) == 1:
            sqlstr += '`{}`\n'.format( colList[-1][0])
        sqlstr += 'FROM `{}.{}.{}`\n'.format(args.project_id, args.bq_dataset_id, args.bq_temp_table)
        sqlstr += 'LIMIT 1\n'

        if args.verbosity > 2:
            print(sqlstr)
    else:
        # Append the entire exported metadata table to the cumulative table
        sqlstr = 'SELECT *\n'
        sqlstr += 'FROM `{}.{}.{}`\n'.format(args.project_id, args.bq_dataset_id, args.bq_temp_table)
        if args.verbosity > 2:
            print(sqlstr)

    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset(args.bq_dataset_id).table(args.bq_cum_table)
    job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.schema_update_options = ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sqlstr,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location="US",
        job_config=job_config,
    )  # API request - starts the query

    query_job.result()  # Wait for the query to finish
    print("Query results loaded to table {}".format(table_ref.path))

def zip_is_done(zip):
    zip in dones
#    series_instance_uid = zip.split('.',2)[2].rsplit('.',1)[0]
#    return series_instance_uid in series

def scanZips(args, api_key):
#    create_dicom_store(args, api_key)
    global zipFileCount
    if args.compact:
        for zip in zips:
            if not zip_is_done(zip):
                if args.verbosity > 1:
                    print("Processing {}".format(zip))
                create_dicom_store(args, api_key, delete=True)
                load_series_into_dicom_store(args, zip, api_key)
                export_dicom_metadata(args, api_key)
                append_to_cumulative_table(args, api_key)

                zipFileCount += 1
            else:
                if args.verbosity > 1:
                    print("Previously done {}".format(zip))
    elif args.incremental:
        if args.load:
            create_dicom_store(args, api_key, delete=args.deleteStore)
            for zip in zips:
                if not zip_is_done(zip):
                    if args.verbosity > 1:
                        print("Processing {}".format(zip))
                    load_series_into_dicom_store(args, zip, api_key)

                    zipFileCount += 1
                else:
                    if args.verbosity > 1:
                        print("Previously done {}".format(zip))
            export_dicom_metadata(args, api_key)
        append_to_cumulative_table(args, api_key)
    else:
        if args.load:
            for zip in zips:
                if not zip_is_done(zip):
                    if args.verbosity > 1:
                        print("Processing {}".format(zip))
                    load_series_into_dicom_store(args, zip, api_key)

                    zipFileCount += 1
                else:
                    if args.verbosity > 1:
                        print("Previously done {}".format(zip))
        export_dicom_metadata(args, api_key)

    #    export_dicom_metadata(args, api_key)
    return zipFileCount

def setup(args):
    http = httplib2.Http()
    http = GoogleCredentials.get_application_default().authorize(http)
    with open(args.api_key) as f:
        api_key = f.read().rstrip()
    create_gh_dataset(args, api_key)
    zips = loadZips(args)
    dones = loadDones(args)
    create_dicom_store(args, api_key, delete=args.deleteStore)
#    series = dicomweb_search_series(args)
    return (zips, http, api_key, dones)

def parse_args():
    parser = argparse.ArgumentParser(description="Build DICOM image metadata table")
    parser.add_argument("-v", "--verbosity", action="count", default=2, help="increase output verbosity")
    parser.add_argument("-z", "--zips", type=str, help="path to file of zip files in GCS to process",
                        default='./zips.txt')
    parser.add_argument("-base_url", type=str, help="Healthcare URL",
                        default='https://healthcare.googleapis.com/v1alpha')
    parser.add_argument("-d", "--dones", type=str, help="path to file containing names of processed series",
                        default='./dones.txt')
    parser.add_argument("--excludes", type=str, help="path to file containing column names to exclude",
                        default='./excludes.txt')
    parser.add_argument("--project_id", type=str, help="Project ID",
                        default='cgc-05-0011')
    parser.add_argument("--bq_dataset_id", type=str, help="BQ metadata dataset",
                        default='working')
    parser.add_argument("--bq_temp_table", type=str, help="Temporary BQ metadata table",
                        default='dicom_temp_metadata_ghc')
    parser.add_argument("--bq_cum_table", type=str, help="Cumulative_BQ metadata table",
                        default='dicom_cum_metadata_ghc')
    parser.add_argument("--location", type=str, help="Google Healthcare location",
                        default='us-central1')
    parser.add_argument("--gh_dataset_id", type=str, help="Google Healthcare dataset",
                        default='cgc-05-0011-metadata-upload-dataset')
    parser.add_argument("--gh_dicom_store_id", type=str, help="Google Healthcare data store",
                        default='cgc-05-0011-dicomstore')
    parser.add_argument("-s", "--service_account", type=str, help="File containing service account",
                        default='/Users/BillClifford/Documents/RadiologyImaging-67b73cac922b.json')
    parser.add_argument("-a", "--api_key", type=str, help="API key",
                        default='/Users/BillClifford/Documents/api_key.txt')
    parser.add_argument("--scratch", type=str, help="path to scratch directory",
                        default='.')
    parser.add_argument("--compact", dest='compact', type=lambda x:bool(distutils.util.strtobool(x)), help="True to generate a row per series; False to generate a row per instance",
                        default=False)
    parser.add_argument("--incremental", dest='incremental', type=lambda x:bool(distutils.util.strtobool(x)), help="True: export metadata to temp table, then append to cum table; False just export to 'temp' table ",
                        default=False)
    parser.add_argument("--deleteStore", dest='deleteStore', type=lambda x:bool(distutils.util.strtobool(x)), help="True: delete datastore before creating new one",
                        default=False)
    parser.add_argument('--load', dest='load', type=lambda x:bool(distutils.util.strtobool(x)), help="True: load all files to datastore; False just export to 'temp' table ",
                        default=True)

    return parser.parse_args()


if __name__ == '__main__':

    args = parse_args()
    print(args)

    # Initialize work variables from previously generated data in files
    zips, http, api_key, dones = setup(args)

    t0 = time.time()
    fileCount = scanZips(args, api_key)
    t1 = time.time()

    if args.verbosity > 0:
        print("{} zip files processed in {} seconds".format(fileCount, t1 - t0),
              file=sys.stderr)
