#!/usr/bin/env python3

import os
import json
import shutil
import logging
import subprocess
from argparse import ArgumentParser

import boto3
import yaml

from unity_app_generator import interface as build_interface

from ..mdps.tool import API_Tool

# Deploy artfacts back to this repo
DEPLOY_BASE_DIR = os.path.realpath(os.path.dirname(__file__))
APP_STATE_DIR = os.path.join(DEPLOY_BASE_DIR, ".app_state")

DOCKER_IMAGE_NAMESPACE = "tropess"
DEFAULT_DOCKER_IMAGE_TAG = "latest"

# Describes which repositories to build and their default locations
# These can get overriden on the command line
SOURCE_REPOS = {
    "muses_ingest": "git@github.jpl.nasa.gov:MUSES-Processing/mdps-muses-data-ingest.git",
    "py_tropess":"git@github.jpl.nasa.gov:MUSES-Processing/py-tropess.git", 
}

# Directories under DEPLOY_BASE_DIR where to put artifact files
ARTIFACT_DIRS = {
    "muses_ingest": "mdps-muses-data-ingest",
    "py_tropess": "py-tropess",
}

SOURCE_CWL_ARTIFACT_FILENAME = "process.cwl"
DEST_CWL_ARTIFACT_FILENAME = "process-{project_name}-{venue_name}.cwl"
EXAMPLE_JOB_INPUT_FILENAME = "example_job_input.json"

logger = logging.getLogger()

class DeployApp(API_Tool):

    def __init__(self, deploy_base_dir=None, **vargs, **kwargs):
        super().__init__(*vargs, **kwargs)

        assert deploy_base_dir is not None
        self.deploy_base_dir = deploy_base_dir

    def __init__(self, app_name, env_config_file=None, **kwargs):
        super().__init__(env_config_file=env_config_file)

        self.app_name = app_name

        self._verify_project_venue_name(self.mdps_project, self.mdps_venue)

    def _verify_project_venue_name(self, project_name, venue_name):
        "Load MDPS venue name from AWS parameter store"

        client = boto3.client('ssm')
        response = client.get_parameter(Name=f"/unity/{project_name}/{venue_name}/venue-name")

        assert(response['Parameter']['Value'] == venue_name)

    @property
    def app_state_dir(self):
        "Where artifacts for unity-app-generator are stored"
        
        # Keep a seperate state dir for each application
        state_dir = os.path.join(APP_STATE_DIR, self.app_name)
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        return state_dir

    def init_repo(self, source_repo):
        "Handle copying of Git repository from source location"

        # Allow version tags for default repo
        checkout_tag = None
        if source_repo[0] == "@":
            checkout_tag = source_repo[1:]
            source_repo = SOURCE_REPOS[self.app_name]

        if checkout_tag is not None:
            logger.info(f"Initializing {self.app_name} from {source_repo} @ {checkout_tag} for {self.mdps_project}/{self.mdps_venue}")
        else:
            logger.info(f"Initializing {self.app_name} from {source_repo} for {self.mdps_project}/{self.mdps_venue}")

        # Clone repo into state dir
        checkout_dir = os.path.join(self.app_state_dir, "repo")

        # Remove previous copy to prevent overwrites
        if os.path.exists(checkout_dir) and os.path.realpath(source_repo) != os.path.realpath(checkout_dir):
            logger.debug(f"Removing existing checkout dir {checkout_dir}")
            shutil.rmtree(checkout_dir)

        build_interface.init(self.app_state_dir, source_repo, checkout_dir, checkout=checkout_tag)

    def build_app(self, image_tag=None):
        "Build Docker image to be used for venue deployment"

        logger.info(f"Building {self.app_name} for {self.mdps_project}/{self.mdps_venue}")

        # Build a local Docker image
        build_interface.build_docker(self.app_state_dir,
                                     image_namespace=DOCKER_IMAGE_NAMESPACE,
                                     image_repository=ARTIFACT_DIRS[self.app_name],
                                     image_tag=image_tag)

    def deploy_for_venue(self):

        logger.info(f"Pushing Docker image for {self.app_name} into {self.mdps_project}/{self.mdps_venue}")

        # Push the image into ECR to set the remote URL
        build_interface.push_ecr(self.app_state_dir)

    def update_artifacts(self):
        
        logger.info(f"Capturing {self.app_name} artifacts for {self.mdps_project}/{self.mdps_venue}")
        
        # Create CWL file
        build_interface.build_cwl(self.app_state_dir)

        # Copy the CWL file into a location within the repo matching the project/venue names
        app_artifact_dirname = ARTIFACT_DIRS[self.app_name]
        source_cwl_fn = os.path.join(self.app_state_dir, "cwl", SOURCE_CWL_ARTIFACT_FILENAME)

        dest_filename = DEST_CWL_ARTIFACT_FILENAME.format(project_name=self.mdps_project, venue_name=self.mdps_venue)
        dest_cwl_fn = os.path.join(DEPLOY_BASE_DIR, app_artifact_dirname, dest_filename)

        shutil.copyfile(source_cwl_fn, dest_cwl_fn)

        # Recreate example_job_input.json
        # Use make-template from cwl tool on the CWL we created
        # Read in that YAML and merge with existing JSON
        example_job_filename = os.path.join(app_artifact_dirname, EXAMPLE_JOB_INPUT_FILENAME)
        if os.path.exists(example_job_filename):
            with open(example_job_filename, "r") as ex_file:
                ex_inp_contents = json.load(ex_file)
        else:
            ex_inp_contents = None

        yaml_output = subprocess.check_output(f"cwltool --make-template {dest_cwl_fn}", shell=True)
        yaml_contents = yaml.safe_load(yaml_output)

        # Update template with existing file so any manually modified values are preserved
        if ex_inp_contents is not None:
            yaml_contents.update(ex_inp_contents)
        else:
            ex_inp_contents = yaml_contents

        with open(example_job_filename, "w") as ex_file:
            json.dump(ex_inp_contents, ex_file, indent=2)

def main():

    parser = ArgumentParser(description="Build TROPESS apps for deployment in MDPS")

    default_app_list = list(SOURCE_REPOS.keys())
    parser.add_argument("app", nargs="*",
        help=f"Applications to build other than default of all: {default_app_list}")

    for app_name, default_repo in SOURCE_REPOS.items():
        parser.add_argument(f"--{app_name}", default=default_repo,
                            help=f"Location of {app_name} source repository other than default: {default_repo}. Prefix a tag with @ to use a specific version at the default location.")

    parser.add_argument("-t", "--tag", dest="docker_tag", default=DEFAULT_DOCKER_IMAGE_TAG,
        help=f"Docker tag to use for images instead of default: {DEFAULT_DOCKER_IMAGE_TAG}")
    
    parser.add_argument("--skip-build", action="store_true",
        help="Skip Dockerfile build if has been built with previous execution of this program")

    parser.add_argument("--deployment_dir", dest="deploy_base_dir", default=os.curdir,
        help="Location where CWL artifacts are deployed")

    parser.add_argument("--verbose", "-v", action="store_true", default=False,
        help=f"Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.app is None or len(args.app) == 0:
        app_list = default_app_list
    else:
        app_list = args.app

    for app_name in app_list:
        if app_name not in SOURCE_REPOS:
            raise Exception(f"Unknown application: {app_name}")

        app_deploy = DeployApp(app_name)
        app_deploy.init_repo(getattr(args, app_name))
        if not args.skip_build:
            app_deploy.build_app(args.docker_tag)
        app_deploy.deploy_for_venue()
        app_deploy.update_artifacts()

if __name__ == '__main__':
    main()