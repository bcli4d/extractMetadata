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

from googleapiclient import discovery
from google.oauth2 import service_account
#from requests import HTTPError

from google.auth.transport import requests
from googleapiclient.errors import HttpError

from email import encoders
#from email.mime.application import MIMEApplication
import email.mime.application as application
#from email.mime.multipart import MIMEMultipart
import email.mime.multipart as multipart
import email

zipFileCount = 0
zips = set()
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
        args.project_id, args.location, args.dataset_id)

    datasets = client.projects().locations().datasets()
    dataset = datasets.get(name=dataset_name).execute()

    print('Name: {}'.format(dataset.get('name')))
    print('Time zone: {}'.format(dataset.get('timeZone')))

    return dataset

def createDataset(args,
        api_key):
    """Creates a dataset."""
    dataset = get_dataset(args, api_key)
    if dataset['name'].find(args.dataset_id) < 0 :
        client = get_client(args, api_key)
        dataset_parent = 'projects/{}/locations/{}'.format(
            args.project_id, args.location)

        body = {}

        request = client.projects().locations().datasets().create(
            parent=dataset_parent, body=body, datasetId=args.dataset_id)

        try:
            response = request.execute()
            print('Created dataset: {}'.format(dataset_id))
            return response
        except HttpError as e:
            print('Error, dataset not created: {}'.format(e))
            return ""

def list_dicom_stores(args, api_key):
    """Lists the DICOM stores in the given dataset."""
    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(
        args.project_id, args.location, args.dataset_id)
    dicom_store_name = '{}/dicomStores/{}'.format(
        dicom_store_parent, args.dicom_store_id)

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

    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(
        args.project_id, args.location, args.dataset_id)
    dicom_store_name = '{}/dicomStores/{}'.format(
        dicom_store_parent, args.dicom_store_id)

    request = client.projects().locations().datasets().dicomStores().delete(name=dicom_store_name)

    try:
        response = request.execute()
        print('Deleted DICOM store: {}'.format(args.dicom_store_id))
        return response
    except HttpError as e:
        print('Error, DICOM store not deleted: {}'.format(e))
        return ""


def create_dicom_store(args, api_key):
    """Creates a new DICOM store within the parent dataset."""
    dicom_stores = list_dicom_stores(args, api_key)

    if len(dicom_stores) > 0 :
        delete_dicom_store(args, api_key)

    client = get_client(args, api_key)
    dicom_store_parent = 'projects/{}/locations/{}/datasets/{}'.format(args.project_id, args.location, args.dataset_id)

    body = {}

    request = client.projects().locations().datasets().dicomStores().create(
        parent=dicom_store_parent, body=body, dicomStoreId=args.dicom_store_id)

    try:
        response = request.execute()
        print('Created DICOM store: {}'.format(args.dicom_store_id))
        return response
    except HttpError as e:
        print('Error, DICOM store not created: {}'.format(e))
        return ""


    # Add name of processed file
def appendDones(args, zip):
    with open(args.dones, 'a') as f:
        f.write("{}\n".format(zip))

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
    delete_dicom_store(args, api_key)

    zipfileName = join(args.scratch,'dcm.zip')
    dicomDirectory = join(args.scratch,'dicoms')
    shutil.rmtree(dicomDirectory)
    os.remove(zipfileName)

def wait_for_operation_completion(args, api_key, path, timeout):

    """Export metadata to a BQ table"""
    client = get_client(args, api_key)

    request = client.projects().locations().datasets().operations().get(name=path)

    success = False
    while time.time() <timeout:
        print('Waiting for operation completion...')
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


def wait_for_operation_completion1(path, timeout):
    success = False
    while time.time() < timeout:
        print('Waiting for operation completion...')
        resp, content = http.request(path, method='GET')
        assert resp.status == 200, 'error polling for Operation results, code: {0}, response: {1}'.format(resp.status,
                                                                                                          content)
        response = json.loads(content)
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
        args.project_id, args.location, args.dataset_id)
    dicom_store_name = '{}/dicomStores/{}'.format(
        dicom_store_parent, args.dicom_store_id)

    body = {
      'outputConfig': {
        'bigQueryDestination': {
            'dataset': args.bq_dataset,
            'table': args.bq_table,
            'overwriteTable': 'TRUE'
        }
      }
    }

    request = client.projects().locations().datasets().dicomStores().export(
        name=dicom_store_name, body=body)

    try:
        response = request.execute()
        print('Exported DICOM metadata to table {}.{}: '.format(args.bq_dataset, args.bq_table))
        metadata_operation_name = response['name']

        timeout = time.time() + 10 * 60  # Wait up to 10 minutes.
        path = join(HEALTHCARE_API_URL, metadata_operation_name)
        path = metadata_operation_name
        response = wait_for_operation_completion(args, api_key, path, timeout)

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

# Store a DICOM instance in the datastore
def dicomweb_store_instance(args, dcm_file):

    """ Handles the POST requests specified in the DICOMweb standard. """

    base_url = "https://healthcare.googleapis.com/v1alpha"
    url = '{}/projects/{}/locations/{}'.format(base_url,
                                               args.project_id, args.location)

    dicomweb_path = '{}/datasets/{}/dicomStores/{}/dicomWeb/studies'.format(
        url, args.dataset_id, args.dicom_store_id)

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
        print('Stored DICOM instance:')
        print(response.text)
        return response
    except HttpError as err:
        print(err)
        return ""


def processSeries(args, zip, api_key):
    create_dicom_store(args, api_key)
    zipFilesPath = getZipFromGCS(args, zip)

    dicoms = os.listdir(zipFilesPath)
    dicoms.sort()

    for dicom in dicoms:
        dicomweb_store_instance(args, join(zipFilesPath,dicom))
    export_dicom_metadata(args, api_key)

    appendDones(args, zip)
    cleanupSeries(args, api_key)


# Extract metadata from specified set of files in GCS
def scanZips(args, api_key):
    global zipFileCount
    for zip in zips:
        if not zip in dones:
            processSeries(args, zip, api_key)
            zipFileCount += 1
        else:
            if args.verbosity > 1:
                print("Previously done {}".format(zip))
    return zipFileCount

def setup(args):
    http = httplib2.Http()
    http = GoogleCredentials.get_application_default().authorize(http)
    with open(args.api_key) as f:
        api_key = f.read().rstrip()
    createDataset(args, api_key)
    dones = loadDones(args)
    zips = loadZips(args)

    return (dones, zips, http, api_key)

def parse_args():
    parser = argparse.ArgumentParser(description="Build DICOM image metadata table")
    parser.add_argument("-v", "--verbosity", action="count", default=2, help="increase output verbosity")
    parser.add_argument("-z", "--zips", type=str, help="path to file of zip files in GCS to process",
                        default='./zips.txt')
    parser.add_argument("-d", "--dones", type=str, help="path to file containing names of processed series",
                        default='./dones.txt')
    parser.add_argument("--project_id", type=str, help="Project ID",
                        default='cgc-05-0011')
    parser.add_argument("--bq_dataset", type=str, help="BQ metadata dataset",
                        default='working')
    parser.add_argument("--bq_table", type=str, help="BQ metadata table",
                        default='dicom_metadata_ghc')
    parser.add_argument("--location", type=str, help="Google Healthcare location",
                        default='us-central1')
    parser.add_argument("--dataset_id", type=str, help="Google Healthcare dataset",
                        default='cgc-05-0011-metadata-upload-dataset')
    parser.add_argument("--dicom_store_id", type=str, help="Google Healthcare data store",
                        default='cgc-05-0011-dicomstore')
    parser.add_argument("--service_account", type=str, help="File containing service account",
                        default='/Users/BillClifford/Documents/RadiologyImaging-67b73cac922b.json')
 #   parser.add_argument("--service_account", type=str, help="File containing service account",
 #                       default='/Users/BillClifford/Downloads/RadiologyImaging-ea433bc1d422.json')
    parser.add_argument("--api_key", type=str, help="API key",
                        default='/Users/BillClifford/Documents/api_key.txt')
    parser.add_argument("-s", "--scratch", type=str, help="path to scratch directory",
                        default='.')

    return parser.parse_args()


if __name__ == '__main__':

    args = parse_args()

    # Initialize work variables from previously generated data in files
    dones, zips, http, api_key = setup(args)

    t0 = time.time()
    fileCount = scanZips(args, api_key)
    t1 = time.time()

    if args.verbosity > 0:
        print("{} zip files processed in {} seconds".format(fileCount, t1 - t0),
              file=sys.stderr)
