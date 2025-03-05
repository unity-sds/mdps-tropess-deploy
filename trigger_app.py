#!/usr/bin/env python3
"""
"""

import os
import json

import argparse

from datetime import datetime, timezone
from pprint import pprint

import logging

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse

from unity_sds_client.unity import Unity
from unity_sds_client.unity import UnityEnvironments

import boto3
from dotenv import load_dotenv

REQUEST_INSTANCE_TYPE = "t3.medium"
REQUEST_STORAGE = "10Gi"

DAG_NAME="cwl_dag_modular"

REPO_BASE_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger()

# JSON files relative to this directory with default values in the form of a CWL parameters file
DEFAULT_JOB_PARAMETER_FILE = {
    "data_ingest": "mdps-muses-data-ingest/example_job_input.json",
}

# Sub directories under current directory where we can find deployment files
SUBCOMMAND_DIRS = {
    "data_ingest": "mdps-muses-data-ingest",
    "py_tropess": "py_tropess",
}

# URL base for files given to Airflow
DEPLOY_FILES_BASE_URL = "https://raw.githubusercontent.com/unity-sds/mdps-tropess-deploy/refs/heads/main/"

CWL_WORKFLOW_FILENAME = "{subcommand_dir}/process-{project}-{venue}.cwl"

# For verification of S3 URL
EXPECTED_INGEST_SUBDIRS = ["L2_Products", "L2_Products_Lite"]

def read_job_file(sub_command):
    param_filename = os.path.join(REPO_BASE_DIR, DEFAULT_JOB_PARAMETER_FILE[sub_command])

    with open(param_filename, "r") as param_file:
        return json.load(param_file)

class TropessDAGRunner(object):

    def __init__(self, env_config_file=None, **kwargs):

        # Load environment variables from a .env file
        load_dotenv(dotenv_path=env_config_file)

        self.unity = self.login_unity()

    def login_unity(self):

        mdps_project = os.environ.get("PROJECT", "unity")
        mdps_venue = os.environ.get("VENUE", "ops")
        mdps_env = os.environ.get("ENVIRONMENT", "PROD")

        logger.info(f"Logging into Unity/MDPS with project = {mdps_project}, venue = {mdps_venue}, environment = {mdps_env}")

        env = UnityEnvironments[mdps_env]
        s = Unity(environment=env)
        s.set_project(mdps_project)
        s.set_venue(mdps_venue)

        return s

    def trigger_dag(self, process_workflow, process_args, stac_json, use_ecr=True):

        # get airflow host,user,pwd from ENV variables
        if "AIRFLOW_HOST" in os.environ:
            airflow_host = os.getenv("AIRFLOW_HOST")
        else:
            airflow_host = self.unity._session.get_unity_href() + self.unity._session.get_venue_id() + "/sps/"

        airflow_username = os.getenv("AIRFLOW_USERNAME")
        airflow_password = os.getenv("AIRFLOW_PASSWORD")

        url = os.path.join(airflow_host, f"api/v1/dags/{DAG_NAME}/dagRuns")

        logger.info(f"Triggering AirFlow at: {url}")

        headers = {
            "Content-type": "application/json", 
            "Accept": "text/json",
        }

        dt_now = datetime.now(timezone.utc)
        logical_date = dt_now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # data = {"logical_date": logical_date}
        # Example on how to pass DAG specific parameters
        data = {
            "logical_date": logical_date,
            "conf": {
                "process_args": json.dumps(process_args),
                "process_workflow": process_workflow,
                "stac_json": stac_json,
                "request_instance_type": REQUEST_INSTANCE_TYPE,
                "request_storage": REQUEST_STORAGE,
                "use_ecr": use_ecr,
            },
        }

        result = requests.post(
            url, json=data, headers=headers, auth=HTTPBasicAuth(airflow_username, airflow_password),
            timeout=15,
        )

        result_json = result.json()
        logger.debug("Response JSON:")
        logger.debug(result_json)

        if result.status_code != 200:
            raise Exception(f"Error triggering Airflow DAG at {url}: {result.text}")

    def _verify_s3_path(self, base_path, data_path):

        s3 = boto3.client('s3')

        # Path must end with slash to see it as a "directory"
        if not data_path.endswith('/'):
            data_path += "/"

        # Put parts of URL together then parse for boto3 call        
        url_full_path = os.path.join(base_path, data_path)
        url_parts = urlparse(url_full_path, allow_fragments=False)

        # Query for contents of the URL if any
        resp = s3.list_objects(Bucket=url_parts.netloc,
                               Prefix=url_parts.path.lstrip("/"), Delimiter='/')

        common_prefixes = resp.get('CommonPrefixes', [])

        # Verify the path exists and it contains files        
        if 'Contents' not in resp:
            raise Exception(f"Could not find anything at S3 URL: {url_full_path}")

        if len(common_prefixes) == 0:
            raise Exception(f"No files or directories found at {url_full_path}")

        # Verify expected items are located at the path
        path_sub_items = [ os.path.basename(p['Prefix'].rstrip("/")) for p in common_prefixes ]

        for expected_dir in EXPECTED_INGEST_SUBDIRS:
            if expected_dir not in path_sub_items:
                raise Exception(f"Did not find {expected_dir} under {url_full_path}")

        # Log what we found at the S3 path
        logger.info("Ingesting data from S3 path: {url_full_path}")
        logger.info("Path contains:")
        for sub_dir in path_sub_items:
            logger.info(f" - {sub_dir}")


    def _verify_file_url(self, url):

        if response := requests.get(url).status_code != 200:
            raise Exception(f"Invalid file url: {url}, get failed with status code: {response.status_code}")

    def data_ingest(self, input_data_ingest_path, collection_group_keyword, input_data_base_path, collection_version, trigger=False,  **kwargs):

        assert(input_data_ingest_path is not None)
        assert(collection_group_keyword is not None)
        assert(input_data_base_path is not None)
        assert(collection_version is not None)

        # Verify the S3 path is accessible
        self._verify_s3_path(input_data_base_path, input_data_ingest_path)
        
        process_args = {
            "input_data_ingest_path": input_data_ingest_path,
            "collection_group_keyword": collection_group_keyword,
            "input_data_base_path": input_data_base_path,
            "collection_version": collection_version,
        }
        
        # Find the process.cwl file for the current project/venue
        # Verify that it exists locally before assuming the URL we construct is valid
        process_workflow_filename = CWL_WORKFLOW_FILENAME.format(
            subcommand_dir=SUBCOMMAND_DIRS["data_ingest"],
            project=self.unity._session._project,
            venue=self.unity._session._venue,
        ) 

        if not os.path.exists(os.path.join(REPO_BASE_DIR, process_workflow_filename)):
            raise Exception(f"Could not find process CWL file: {process_workflow_filename}")
        
        process_workflow_url = os.path.join(DEPLOY_FILES_BASE_URL, process_workflow_filename)

        if response := requests.get(process_workflow_url).status_code != 200:
            raise Exception(f"Invalid process CWL url: {process_workflow_url}, get failed with status code: {response.status_code}")

        self._verify_file_url(process_workflow_url)

        logger.info(f"Using worflow CWL: {process_workflow_url}")
        
        # Use the empty stac_json stored in the repo
        stac_json_url = os.path.join(
            DEPLOY_FILES_BASE_URL,
            SUBCOMMAND_DIRS["data_ingest"],
            "stage_in.json"
        )

        self._verify_file_url(stac_json_url)
        logger.info(f"Using STAC JSON: {stac_json_url}")

        # With verification done, trigger the Airflow run
        if trigger:
            self.trigger_dag(process_workflow_url, process_args, stac_json_url, use_ecr=True)

    def py_tropess(self, **kwargs):
        pass

def main():

    parser = argparse.ArgumentParser(description="Trigger TROPESS processing in MDPS")

    parser.add_argument("--debug", action="store_true", default=False,
        help=f"Enable verbose debug logging")

    parser.add_argument("--trigger", action="store_true", default=False,
        help="Unless specified the AirFlow is not trigger, instead a dry run is done")

    subparsers = parser.add_subparsers(required=True, dest='subparser_name')

    # data_ingest
    parser_ingest = subparsers.add_parser('data_ingest',
        help=f"Schedule ingestion of data from the TROPESS S3 bucket into MDPS")

    parser_ingest.add_argument("-i", "--input_path", dest="input_data_ingest_path", required=True,
        help="Path under base S3 path with files to be ingested")
    
    parser_ingest.add_argument("-k", "--keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being ingested")
     
    parser_ingest.add_argument("-b", "--base_path", dest="input_data_base_path", required=False,
        help="Base S3 URL path where data is sourced from")
    
    parser_ingest.add_argument("-v", "--verbose", dest="collection_version", required=False,
        help="Collection version for the data being ingested")
       
    parser_ingest.set_defaults(func=TropessDAGRunner.data_ingest)

    # final argument processing
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    args_dict = vars(args)
    
    dag_trigger = TropessDAGRunner(**args_dict)

    command_args = read_job_file(args.subparser_name)
    command_args.update({ k:v for k,v in args_dict.items() if v is not None})

    args.func(dag_trigger, **command_args)

if __name__ == "__main__":
    main()
