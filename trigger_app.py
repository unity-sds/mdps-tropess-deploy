#!/usr/bin/env python3
"""
"""

import os
import re
import json

import argparse

from datetime import datetime, timezone
from pprint import pprint, pformat

import logging

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse

from unity_sds_client.unity import Unity
from unity_sds_client.unity import UnityEnvironments
from unity_sds_client.unity_services import UnityServices as services
from unity_sds_client.resources.collection import Collection

import boto3
from dotenv import load_dotenv
import dateparser

# Import both because the first initiates begining in the config
import tropess_product_spec.config as tps_config
from tropess_product_spec.config import collection_groups
from tropess_product_spec.schema import CollectionGroup

REQUEST_INSTANCE_TYPE = "t3.medium"
REQUEST_STORAGE = "10Gi"

DEFAULT_DAG_NAME="cwl_dag_modular_unity"

REPO_BASE_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger()

# JSON files relative to this directory with default values in the form of a CWL parameters file
DEFAULT_JOB_PARAMETER_FILE = {
    "data_ingest": "mdps-muses-data-ingest/example_job_input.json",
    "py_tropess": "py-tropess/example_job_input.json",
}

# Sub directories under current directory where we can find deployment files
SUBCOMMAND_DIRS = {
    "data_ingest": "mdps-muses-data-ingest",
    "py_tropess": "py-tropess",
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

    def _airflow_api_url(self):
        "Load Airflow API URL from SSM parameter store"

        project_name = self.unity._session._project
        venue_name = self.unity._session._venue

        client = boto3.client('ssm')
        response = client.get_parameter(Name=f"/{project_name}/{venue_name}/sps/processing/airflow/api_url")

        return response['Parameter']['Value']

    def trigger_dag(self, process_workflow, run_id, process_args, stac_json, use_ecr=True, use_stac_auth=True, trigger=False):

        # get airflow host,user,pwd from ENV variables
        if "AIRFLOW_API_URL" in os.environ:
            airflow_api_url = os.getenv("AIRFLOW_API_URL")
        else:
            airflow_api_url = self._airflow_api_url()

        dag_name = os.getenv("AIRFLOW_DAG_NAME", DEFAULT_DAG_NAME)

        trigger_url = os.path.join(airflow_api_url, f"dags/{dag_name}/dagRuns")

        logger.info(f"Using Airflow API URL: {trigger_url}")

        headers = {
            "Content-type": "application/json", 
            "Accept": "text/json",
            "Authorization": "Bearer " + self.unity._session.get_auth().get_token(),
        }

        dt_now = datetime.now(timezone.utc)
        logical_date = dt_now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # data = {"logical_date": logical_date}
        # Example on how to pass DAG specific parameters
        data = {
            "dag_run_id": run_id,
            "logical_date": logical_date,
            "conf": {
                "process_args": json.dumps(process_args),
                "process_workflow": process_workflow,
                "stac_json": stac_json,
                "request_instance_type": REQUEST_INSTANCE_TYPE,
                "request_storage": REQUEST_STORAGE,
                "use_ecr": use_ecr,
                "unity_stac_auth_type": use_stac_auth,
            },
        }

        logger.debug("DAG parameters:")
        logger.debug(pformat(data, indent=2))
        logger.debug("process_args from DAG parameters as one line:")
        logger.debug(data['conf']['process_args'])

        if trigger:
            logger.info(f"Triggering Airflow DAG at: {trigger_url}")

            result = requests.post(
                trigger_url, json=data, headers=headers,
                timeout=15,
            )

            result_json = result.json()
            logger.debug("Response JSON:")
            logger.debug(pformat(result_json, indent=2))

            if result.status_code != 200:
                raise Exception(f"Error triggering Airflow DAG at {trigger_url}: {result.text}")
        else:
            logger.info("Airflow DAG dry-run only")

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
        logger.info(f"Ingesting data from S3 path: {url_full_path}")
        logger.info("Path contains:")
        for sub_dir in path_sub_items:
            logger.info(f" - {sub_dir}")


    def _verify_file_url(self, url):

        if response := requests.get(url).status_code != 200:
            raise Exception(f"Invalid file url: {url}, get failed with status code: {response.status_code}")


    def _process_workflow_url(self, subcommand_name):

        # Find the process.cwl file for the current project/venue
        # Verify that it exists locally before assuming the URL we construct is valid
        process_workflow_filename = CWL_WORKFLOW_FILENAME.format(
            subcommand_dir=SUBCOMMAND_DIRS[subcommand_name],
            project=self.unity._session._project,
            venue=self.unity._session._venue,
        ) 

        if not os.path.exists(os.path.join(REPO_BASE_DIR, process_workflow_filename)):
            raise Exception(f"Could not find process CWL file: {process_workflow_filename}")
        
        process_workflow_url = os.path.join(DEPLOY_FILES_BASE_URL, process_workflow_filename)

        if (response := requests.get(process_workflow_url)).status_code != 200:
            raise Exception(f"Invalid process CWL url: {process_workflow_url}, get failed with status code: {response.status_code}")

        self._verify_file_url(process_workflow_url)

        logger.info(f"Using worflow CWL: {process_workflow_url}")        

        return process_workflow_url

    def data_ingest(self, input_data_ingest_path, collection_group_keyword, input_data_base_path, collection_version, trigger=False,  **kwargs):

        assert(input_data_ingest_path is not None)
        assert(collection_group_keyword is not None)
        assert(input_data_base_path is not None)
        assert(collection_version is not None)

        # Verify the S3 path is accessible
        self._verify_s3_path(input_data_base_path, input_data_ingest_path)

        # Verify the collection_group_keyword
        collection_group_obj = CollectionGroup.get_collection_group(collection_group_keyword)
        if collection_group_obj is None:
            raise Exception(f"Invalid collection_group_keyword: {collection_group_keyword}")
        
        process_args = {
            "input_data_ingest_path": input_data_ingest_path,
            "collection_group_keyword": collection_group_keyword,
            "input_data_base_path": input_data_base_path,
            "collection_version": collection_version,
        }
        
        process_workflow_url = self._process_workflow_url("data_ingest")

        # Use the empty stac_json stored in the repo
        stac_json_url = os.path.join(
            DEPLOY_FILES_BASE_URL,
            SUBCOMMAND_DIRS["data_ingest"],
            "stage_in.json"
        )

        self._verify_file_url(stac_json_url)
        logger.info(f"Using STAC JSON: {stac_json_url}")

        # With verification done, trigger the Airflow run
        run_id = f"TROPESS-data_ingest-{collection_group_keyword}:{input_data_base_path}"
        self.trigger_dag(process_workflow_url, run_id, process_args, stac_json_url, use_ecr=True, use_stac_auth=False, trigger=trigger)

    def _find_sensor_set(self, collection_group_obj, sensor_set_str):
        "Find a sensor set string via straight keyword or from an alias attatched to the collection_group"

        # Sensor Set object from keyword
        sensor_set_obj = tps_config.sensor_sets.get(sensor_set_str, None)
        
        # A collection group has a set of sensor sets that are valid, the mappings are mapped by the string used for the directory structure
        # Try the string with this alias
        if sensor_set_obj is None:
            sensor_set_mapping = collection_group_obj.sensor_set_mappings.get(sensor_set_str, None)
            if sensor_set_mapping is not None:
                sensor_set_obj = sensor_set_mapping.sensor_set
        
        if sensor_set_obj is None:
            raise Exception(f'Could not determine sensor set from string: "{sensor_set_str}"')

        return sensor_set_obj

    def query_data_catalog(self, collection_id, processing_date, stac_output_filename=None, limit=10000):

        logger.info(f"Searching data catalog for MUSES data for collection {collection_id} on date {processing_date}")

        query_filter = f"processing_datetime='{processing_date}'"

        data_manager = self.unity.client(services.DATA_SERVICE)

        stac_query_result = data_manager.get_collection_data(Collection(collection_id), limit=limit, filter=query_filter, output_stac=True)

        if 'features' not in stac_query_result:
            raise Exception(f"Error querying data catalog: {stac_query_result}")

        # Write out STAC file before further checking might exit program
        if stac_output_filename is not None:
            logger.info(f"Writing STAC result to: {stac_output_filename}")
            with open(stac_output_filename, "w") as stage_in_file:
                json.dump(stac_query_result, stage_in_file)

        # Load a list of files found in the STAC results for verification purposes
        nc_files = []
        for feat in stac_query_result['features']:
            nc_files += list(filter(lambda fn: re.search(r'\.nc$', fn), feat['assets'].keys()))

        if len(nc_files) == 0:
            raise Exception(f"Found 0 files to process")

        logger.info(f"Found {len(nc_files)} to process:")
        for fn in nc_files:
            logger.info(f" - {fn}")

        return stac_query_result

    def _catalog_query_url(self, stac_query_result):
        "URL to the data catalog query"

        return stac_query_result['links'][0]['href']

    def py_tropess(self, collection_group_keyword, sensor_set, processing_date, product_type, processing_species, muses_collection_version, granule_version, stage_in_output_filename=None, trigger=False, **kwargs):
        
        # Verify the collection_group_keyword
        collection_group_obj = CollectionGroup.get_collection_group(collection_group_keyword)
        if collection_group_obj is None:
            raise Exception(f"Invalid collection_group_keyword: {collection_group_keyword}")

        # Get sensor_set object from string
        sensor_set_obj = self._find_sensor_set(collection_group_obj, sensor_set)

        # Generate collection ID for data services query
        mdps_project=self.unity._session._project
        mdps_venue=self.unity._session._venue
        mdps_collection_name = f'MUSES-{sensor_set_obj.short_name}-{collection_group_obj.short_name}'
        mdps_collection_id = f"URN:NASA:UNITY:{mdps_project}:{mdps_venue}:{mdps_collection_name}___{muses_collection_version}"

        # Get consistent date string for DS query -> YYYY-MM-DD
        query_date = dateparser.parse(processing_date).strftime("%Y-%m-%d")

        # Get information on files we want to process
        stac_query_result = self.query_data_catalog(mdps_collection_id, query_date, stage_in_output_filename)

        # Now construct arguments for DAG query
        process_args = {
            "product_type": product_type,
            "granule_version": granule_version,
        }
        
        # Only set if not a null or none value
        if processing_species is not None and processing_species != "null":
            process_args['processing_species'] = processing_species
        
        process_workflow_url = self._process_workflow_url("py_tropess")
        stac_json_url = self._catalog_query_url(stac_query_result)

        # Unique identifier formed by inputs
        run_id = f"TROPESS-py_tropess-{collection_group_keyword}-{sensor_set}-{processing_date}-{product_type}"
        if processing_species is not None and processing_species != "null":
            species_id = processing_species.replace(" ", "")
            run_id += f"-{species_id}"

        # With verification done, trigger the Airflow run
        self.trigger_dag(process_workflow_url, run_id, process_args, stac_json_url, use_ecr=True, use_stac_auth=True, trigger=trigger)
        
def main():

    parser = argparse.ArgumentParser(description="Trigger TROPESS processing in MDPS")

    parser.add_argument("--debug", action="store_true", default=False,
        help=f"Enable verbose debug logging")

    parser.add_argument("--trigger", action="store_true", default=False,
        help="Unless specified the Airflow is not trigger, instead a dry run is done")

    subparsers = parser.add_subparsers(required=True, dest='subparser_name')

    # data_ingest
    parser_ingest = subparsers.add_parser('data_ingest',
        help=f"Schedule ingestion of data from the TROPESS S3 bucket into MDPS")

    parser_ingest.add_argument("-i", "--input_path", dest="input_data_ingest_path", required=True,
        help="Path under base S3 path with files to be ingested")
    
    parser_ingest.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being ingested")
     
    parser_ingest.add_argument("-b", "--base_path", dest="input_data_base_path", required=False,
        help="Base S3 URL path where data is sourced from")
    
    parser_ingest.add_argument("-v", "--version", dest="collection_version", required=False,
        help="Collection version for the data being ingested")
       
    parser_ingest.set_defaults(func=TropessDAGRunner.data_ingest)

    # py_tropess

    parser_pyt = subparsers.add_parser('py_tropess',
    help=f"Initiate processing of data through py-tropess")

    parser_pyt.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being processed")

    parser_pyt.add_argument("-s", "--sensor_set", dest="sensor_set", required=True,
        help="Sensor set for MUSES data to processed into TROPESS products")

    parser_pyt.add_argument("-d", "--date", dest="processing_date", required=True,
        help="Calendar date for the MUSES data to processed into TROPESS products")

    parser_pyt.add_argument("-p", "--product", dest="product_type", required=True,
        help="The type of TROPESS product to create, ie summary/standard/full")

    parser_pyt.add_argument("--species", dest="processing_species", required=False,
        help="Comma seperated list of species to generate other than all valid ones")

    parser_pyt.add_argument("--muses_version", dest="muses_collection_version", required=False,
        help="Collection version for the MUSES data being processed", default="1")

    parser_pyt.add_argument( "--tropess_version", dest="granule_version", required=False,
        help="Granule version for the collection ID being delivered to the DAAC")

    parser_pyt.add_argument( "--stage_in_output", dest="stage_in_output_filename", required=False,
        help="Save stage in STAC catalog from the data store into a file")

    parser_pyt.set_defaults(func=TropessDAGRunner.py_tropess)

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
