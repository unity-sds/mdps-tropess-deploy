#!/usr/bin/env python3
"""
"""

import os
import re
import json

import argparse

from datetime import datetime, timezone
from pprint import pformat

import logging

import requests
from urllib.parse import urlparse

from unity_sds_client.resources.collection import Collection

import boto3
import yaml

# Import both because the first initiates beginning in the config
import tropess_product_spec.config as tps_config
from tropess_product_spec.schema import CollectionGroup

from ..data.tool import DataTool

REQUEST_INSTANCE_TYPE = "t3.medium"
REQUEST_STORAGE = "10Gi"

DEFAULT_DAG_NAME="cwl_dag_modular"

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

def read_job_file(sub_command, deploy_base_dir):
    param_filename = os.path.join(deploy_base_dir, DEFAULT_JOB_PARAMETER_FILE[sub_command])

    with open(param_filename, "r") as param_file:
        return json.load(param_file)

class TropessDAGRunner(DataTool):

    def __init__(self, deploy_base_dir=None, *vargs, **kwargs):
        super().__init__(*vargs, **kwargs)

        assert deploy_base_dir is not None
        self.deploy_base_dir = deploy_base_dir

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

            if result.status_code != 200:
                raise Exception(f"Error triggering Airflow DAG at {trigger_url}: {result.text}")

            result_json = result.json()
            logger.debug("Response JSON:")
            logger.debug(pformat(result_json, indent=2))

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

    def _extract_cwl_docker_version(self, cwl_workflow_filename):

        with open(cwl_workflow_filename, "r") as yaml_output:
            yaml_contents = yaml.safe_load(yaml_output)

        docker_url = yaml_contents['requirements']['DockerRequirement']['dockerPull']

        _, docker_version = docker_url.split(':', 2)

        return docker_version

    def _process_workflow_url(self, subcommand_name):

        # Find the process.cwl file for the current project/venue
        # Verify that it exists locally before assuming the URL we construct is valid
        process_workflow_filename = CWL_WORKFLOW_FILENAME.format(
            subcommand_dir=SUBCOMMAND_DIRS[subcommand_name],
            project=self.unity._session._project,
            venue=self.unity._session._venue,
        ) 

        cwl_workflow_filename = os.path.join(self.deploy_base_dir, process_workflow_filename)
        if not os.path.exists(cwl_workflow_filename):
            raise Exception(f"Could not find process CWL file: {process_workflow_filename}")

        docker_version = self._extract_cwl_docker_version(cwl_workflow_filename)
        
        process_workflow_url = os.path.join(DEPLOY_FILES_BASE_URL, process_workflow_filename)

        if (response := requests.get(process_workflow_url)).status_code != 200:
            raise Exception(f"Invalid process CWL url: {process_workflow_url}, get failed with status code: {response.status_code}")

        self._verify_file_url(process_workflow_url)

        logger.info(f"Using worflow CWL: {process_workflow_url}")        

        return process_workflow_url, docker_version

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
        
        process_workflow_url, docker_version = self._process_workflow_url("data_ingest")

        # Use the empty stac_json stored in the repo
        stac_json_url = os.path.join(
            DEPLOY_FILES_BASE_URL,
            SUBCOMMAND_DIRS["data_ingest"],
            "stage_in.json"
        )

        self._verify_file_url(stac_json_url)
        logger.info(f"Using STAC JSON: {stac_json_url}")

        # With verification done, trigger the Airflow run
        run_id = f"TROPESS-data_ingest_{docker_version}-{collection_group_keyword}:{input_data_ingest_path.replace("/", "-")}"
        self.trigger_dag(process_workflow_url, run_id, process_args, stac_json_url, use_ecr=True, use_stac_auth=False, trigger=trigger)

    def query_input_data(self, collection_group, sensor_set_str, muses_collection_version, processing_date, limit=10000):

        muses_collection_ids = self.muses_collection_ids(collection_group, muses_collection_version, sensor_set_str)

        if len(muses_collection_ids) > 1:
            raise Exception(f"Multiple sensor sets for the {collection_group.keyword} collection group, add sensor_set to argument to filter")
        
        stac_query_result = super().query_data_catalog(muses_collection_ids[0], processing_date)

        # Load a list of files found in the STAC results for verification purposes
        nc_files = []
        for feat in stac_query_result['features']:
            nc_files += list(filter(lambda fn: re.search(r'\.nc$', fn), feat['assets'].keys()))

        if len(nc_files) == 0:
            raise Exception(f"Found 0 files to process")

        logger.info(f"Found {len(nc_files)} to process:")
        for fn in nc_files:
            logger.info(f" - {fn}")

        return stac_query_result['links'][0]['href']

    def py_tropess(self, collection_group, processing_date, product_type, processing_species, muses_collection_version, granule_version, sensor_set_str=None, trigger=False, **kwargs):
        
        # Get information on files we want to process
        stac_json_url = self.query_input_data(collection_group, sensor_set_str, muses_collection_version, processing_date)

        # Now construct arguments for DAG query
        process_args = {
            "product_type": product_type,
            "granule_version": granule_version,
        }
        
        # Only set if not a null or none value
        if processing_species is not None and processing_species != "null":
            process_args['processing_species'] = processing_species
        
        process_workflow_url, docker_version = self._process_workflow_url("py_tropess")

        # Unique identifier formed by inputs
        run_id = f"TROPESS-py_tropess_{docker_version}-{collection_group.keyword}-{sensor_set_str}-{processing_date}-{product_type}"
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

    parser.add_argument("--deployment_dir", dest="deploy_base_dir", default=os.curdir,
        help="Location where CWL artifacts are deployed")

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

    parser_pyt.add_argument("-d", "--date", dest="processing_date", required=True,
        help="Calendar date for the MUSES data to processed into TROPESS products")

    parser_pyt.add_argument("-p", "--product", dest="product_type", required=True,
        help="The type of TROPESS product to create, ie summary/standard/full")
    
    parser_pyt.add_argument("-s", "--sensor_set", dest="sensor_set_str", default=None,
        help="Sensor set for MUSES data to processed into TROPESS products, only required when a collection group has multiple sensor sets")

    parser_pyt.add_argument("--species", dest="processing_species", required=False,
        help="Comma seperated list of species to generate other than all valid ones")

    parser_pyt.add_argument("--muses_version", dest="muses_collection_version", required=False,
        help="Collection version for the MUSES data being processed", default="1")

    parser_pyt.add_argument( "--tropess_version", dest="granule_version", required=False,
        help="Granule version for the collection ID being delivered to the DAAC")

    parser_pyt.set_defaults(func=TropessDAGRunner.py_tropess)

    # final argument processing
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    args_dict = vars(args)

    dag_trigger = TropessDAGRunner(**args_dict)

    command_args = read_job_file(args.subparser_name, args.deploy_base_dir)
    command_args.update({ k:v for k,v in args_dict.items() if v is not None})

    # Find collection group object from keyword name
    collection_group_obj = CollectionGroup.get_collection_group(args_dict['collection_group_keyword'])
    if collection_group_obj is None:
        raise Exception(f"Invalid collection_group_keyword: {args_dict['collection_group_keyword']}")
    command_args['collection_group'] = collection_group_obj 

    args.func(dag_trigger, **command_args)

if __name__ == "__main__":
    main()
