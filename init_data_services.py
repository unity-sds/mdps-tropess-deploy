#!/usr/bin/env python3
"""
"""

import os
import logging
import requests

from pprint import pformat

import argparse

from unity_sds_client.unity import Unity
from unity_sds_client.unity import UnityEnvironments
from unity_sds_client.unity_services import UnityServices as services
from unity_sds_client.resources.collection import Collection

# TROPESS packages
from tropess_product_spec.config import collection_group_combinations
from tropess_product_spec.product_naming import format_short_name

from dotenv import load_dotenv

# metadata used within the MDPS data store for TROPESS products
CUSTOM_METADATA_DEF = {
    "tag": {
        "type": "keyword",
    },
    "project": {
        "type": "keyword",
    },
    "short_name": {
        "type": "keyword",
    },
    "long_name": {
        "type": "keyword",
    },
    "doi": {
        "type": "keyword",
    },
    "collection_group": {
        "type": "keyword",
    },
    "product_stage": {
        "type": "keyword",
    },
    "product_type": {
        "type": "keyword",
    },
    "sensor_set": {
        "type": "keyword",
    },
    "species": {
        "type": "keyword",
    },
    "product_version": {
        "type": "keyword",
    },
    "processing_batch": {
        "type": "keyword",
    },
    "processing_profile": {
        "type": "keyword",
    },
    "processing_datetime": {
        "type": "date",
    },
    "retrieval_step": {
        "type": "date",
    }
}

DEFAULT_ARCHIVING_TYPES = [ ".nc" ]

logger = logging.getLogger()

class TropessDataInit(object):

    def __init__(self, env_config_file=None, **kwargs):

        # Load environment variables from a .env file
        load_dotenv(dotenv_path=env_config_file)

        self.mdps_project = os.environ.get("PROJECT", "unity")
        self.mdps_venue = os.environ.get("VENUE", "ops")
        self.mdps_env = os.environ.get("ENVIRONMENT", "PROD")

        self.unity = self.login_unity()
        self.dataManager = self.unity.client(services.DATA_SERVICE)

    def login_unity(self):
        "Initialize unity-sds-client"

        logger.info(f"Logging into Unity/MDPS with project = {self.mdps_project}, venue = {self.mdps_venue}, environment = {self.mdps_env}")

        env = UnityEnvironments[self.mdps_env]
        s = Unity(environment=env)
        s.set_project(self.mdps_project)
        s.set_venue(self.mdps_venue)

        return s

    # %%%%%%%%%%%%%%%%%%%%%%%
    # Register collection ids
    
    def collection_group_short_names(self, collection_group_keyword):
        "Return all TROPESS short names, aka the DAAC collection ID for a collection group"

        short_name_list = []
        for group_kw, product_kw, sensor_set_kw, species_kw in collection_group_combinations(collection_groups_filter=[collection_group_keyword]):
            short_name = format_short_name(group_kw, product_kw, sensor_set_kw, species_kw)
            short_name_list.append(short_name)

        return short_name_list

    def mdps_collection_ids(self, tropess_short_names, collection_version):
        "Generate MDPS collection IDs from TROPESS short names"
    
        # Create a MDPS/Unity collection for each TROPESS product shortname in the collection group
        our_collection_ids = []
        for short_name in tropess_short_names:
            collection_id = f"urn:nasa:unity:{self.mdps_project}:{self.mdps_venue}:{short_name}___{collection_version}"
            our_collection_ids.append(collection_id)
 
        return our_collection_ids
    
    def register_mdps_collection_ids(self, mdps_collection_ids):
        "Register MDPS collection IDs with data services"

        # This is an asynchronous operation, so there may be a delay in the request for a collection creation and when it shows up in the response.
        for mdps_collection_id in mdps_collection_ids:
            logger.info(f"Registering collection id: {mdps_collection_id}")
            self.dataManager.create_collection(Collection(mdps_collection_id))

        logger.info(f"{len(mdps_collection_ids)} collection ids requested")

    def check_registered_collection_ids(self, our_collection_ids):

        if len(our_collection_ids) == 0:
            logger.warning("No TROPESS collection ids to check")
            return

        mdps_collection_ids = []
        for c in self.dataManager.get_collections(limit=1e4):
            mdps_collection_ids.append(c.collection_id)
        
        for collection_id in our_collection_ids:
            if collection_id in mdps_collection_ids:
                logger.info(f"{collection_id} created succesfully")
            else:
                logger.error(f"Collection id {collection_id} is not registered with MDPS")

    def register_collection_ids(self, collection_group_keyword, granule_version, do_update=False, check_update=False, **kwargs):

        tropess_short_names = self.collection_group_short_names(collection_group_keyword)
        mdps_collection_ids = list(self.mdps_collection_ids(tropess_short_names, granule_version))

        if do_update:
            self.register_mdps_collection_ids(mdps_collection_ids)
        else:
            logger.info(f"Shortnames for collection group {collection_group_keyword}: {tropess_short_names}")
            logger.info(f"Generated collection ids: {mdps_collection_ids}")

        if check_update:
            self.check_registered_collection_ids(mdps_collection_ids)

    # %%%%%%%%%%%%%%%
    # Custom Metadata

    def existing_custom_metadata(self, limit=None):
        "Returns metadata fields already defined for MDPS collection ids"

        # Hack an accessor until unity-sds-client supports this
        existing_metadata = {}
        for c in self.dataManager.get_collections(limit=limit):
            url = self.dataManager.endpoint + f"am-uds-dapa/collections/{c.collection_id}/variables"
            token = self.dataManager._session.get_auth().get_token()
            response = requests.get(url, headers={"Authorization": "Bearer " + token})
                
            if response.status_code != 200:
                if hasattr(response, "message"):
                    raise Exception("Error: " + response.message)
                else:
                    raise Exception(f"Error: {response.json()}")
                
            existing_metadata.update(response.json())

        return existing_metadata

    def define_custom_metadata(self, do_update=False, **kwargs):
        "Define custom metadata fields for all future ingested products to the current venue"

        # Custom metadata fields are defined for a given project and venue. The metadata fields can then be used as additional properties in the STAC item file associated with the data. Note that all previously defined custom metadata fields must be included in the call to define_custom_metadata.

        # We query for existing custom metadata to ensure we do not overwrite what has already been defined in our update.
        logger.info("Querying MDPS data services for existing custom metadata")
        existing_metadata_fields = self.existing_custom_metadata()
        custom_metadata_fields = existing_metadata_fields.copy()

        # Update existing metadata with our definitions
        custom_metadata_fields.update(CUSTOM_METADATA_DEF)

        logger.info("Custom metadata fields definition:\n" + pformat(custom_metadata_fields, indent=2))

        if custom_metadata_fields == existing_metadata_fields:
            logger.info("Proposed fields match existing fields")

        # Declare new custom metadata fields
        if do_update:
            logger.info("Commiting custom metadata definition")
            self.dataManager.define_custom_metadata(custom_metadata_fields)
        else:
            logger.info("No custom metadata commited, dry run only")

    def register_daac_delivery(self, **kwargs):
        pass

    # %%%%%%%%%%%%%%%
    # DAAC Archiving

    def get_archive_config(self, collection_id):
        "Returns archive configration for a collection id"

        # Hack an accessor until unity-sds-client supports this
        url = self.dataManager.endpoint + f"am-uds-dapa/collections/{collection_id}/archive"
        token = self.dataManager._session.get_auth().get_token()
        response = requests.get(url, headers={"Authorization": "Bearer " + token})
        
        if response.status_code != 200:
            if hasattr(response, "message"):
                raise Exception("Error: " + response.message)
            else:
                raise Exception(f"Error: {response.json()}")
            
        return response.json()

    def add_archive_config(self, mdps_collection_id, daac_collection_id, daac_data_version, daac_sns_topic_arn, archiving_types=DEFAULT_ARCHIVING_TYPES):

        # Hack an accessor until unity-sds-client supports this
        url = self.dataManager.endpoint + f"am-uds-dapa/collections/{mdps_collection_id}/archive"
        token = self.dataManager._session.get_auth().get_token()

        data = {
            "daac_collection_id": daac_collection_id,
            "daac_data_version": daac_data_version,
            "daac_sns_topic_arn": daac_sns_topic_arn,
            "archiving_types": [{
                "data_type": "text",
                "file_extension": archiving_types,
            }]
        }

        response = requests.put(url, headers={"Authorization": "Bearer " + token}, json=data)
        
        if response.status_code != 200:
            if hasattr(response, "message"):
                raise Exception("Error: " + response.message)
            else:
                raise Exception(f"Error: {response.json()}")
            
        return response.json()

    def delete_archive_config(self, mdps_collection_id, daac_collection_id):

        # Hack an accessor until unity-sds-client supports this
        url = self.dataManager.endpoint + f"am-uds-dapa/collections/{mdps_collection_id}/archive"
        token = self.dataManager._session.get_auth().get_token()

        data = {
            "daac_collection_id": daac_collection_id,
        }

        response = requests.delete(url, headers={"Authorization": "Bearer " + token}, json=data)
        
        if response.status_code != 200:
            if hasattr(response, "message"):
                raise Exception("Error: " + response.message)
            else:
                raise Exception(f"Error: {response.json()}")
            
        return response.json()

    def register_daac_archiving(self, collection_group_keyword, granule_version, sns_arn, do_update=False, delete=False, **kwargs):

        tropess_short_names = self.collection_group_short_names(collection_group_keyword)
        mdps_collection_ids = list(self.mdps_collection_ids(tropess_short_names, granule_version))        
        
        if delete:
            for daac_id, mdps_id in zip(tropess_short_names, mdps_collection_ids):
                logger.info(f"Deleting DAAC archive id: {daac_id} to {mdps_id}")
                self.delete_archive_config(mdps_id, daac_id)
 
        if do_update:
            for daac_id, mdps_id in zip(tropess_short_names, mdps_collection_ids):
                logger.info(f"Registering DAAC archive id: {daac_id} to {mdps_id}")
                self.add_archive_config(mdps_id, daac_id, granule_version, sns_arn)

        for collection_id in mdps_collection_ids:
            archive_cfg = self.get_archive_config(collection_id)
            logger.info(f"Archive config for {collection_id}:\n" + pformat(archive_cfg, indent=2))

# %%%%%%%%%%%%%%%
# Main

def main():

    parser = argparse.ArgumentParser(description="Register a TROPESS collection group with MDPS")

    parser.add_argument("--debug", action="store_true", default=False,
        help=f"Enable verbose debug logging")

    parser.add_argument("-u", "--do_update", action="store_true", default=False,
        help="Perform data services update instead of performing a dry run")

    subparsers = parser.add_subparsers(required=True, dest='subparser_name')

    # register collection_id
    parser_register = subparsers.add_parser('register_collection',
        help=f"Registers a TROPESS collection ID with MDPS data services")

    parser_register.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being ingested")

    parser_register.add_argument("-v", "--tropess_version", dest="granule_version", required=True,
        help="Granule version for the collection ID being delivered to the DAAC")

    parser_register.add_argument("--check", dest="check_update", action="store_true", default=False,
        help="Check that generated MDPS collection ids are registered")

    parser_register.set_defaults(func=TropessDataInit.register_collection_ids)

    # register custom metadata
    
    parser_metadata = subparsers.add_parser('custom_metadata',
        help=f"Registers custom metadata needed by py_tropess output")
    
    parser_metadata.set_defaults(func=TropessDataInit.define_custom_metadata)

    # register DAAC archive delivery
    
    parser_archive = subparsers.add_parser('register_archive',
        help=f"Registers TROPESS collection IDs to be delivered to the DAAC for archiving")

    parser_archive.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being ingested")

    parser_archive.add_argument("-v", "--tropess_version", dest="granule_version", required=True,
        help="Granule version for the collection ID being delivered to the DAAC")
    
    parser_archive.add_argument("-a", "--sns_arn", dest="sns_arn", required=True,
        help="DAAC SNS topic ARN where delivery messages are sent")

    parser_archive.add_argument("--delete", dest="delete", action="store_true", default=False,
        help="Delete DAAC archive configs before creating, or delete configs if not commiting updates")

    parser_archive.set_defaults(func=TropessDataInit.register_daac_archiving)
     
    # final argument processing
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    args_dict = vars(args)

    data_init = TropessDataInit(**args_dict)

    args.func(data_init, **args_dict)

if __name__ == "__main__":
    main()