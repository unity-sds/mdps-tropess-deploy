#!/usr/bin/env python3
"""
"""

import os
import re

import dateparser

import argparse

from prettytable import PrettyTable

import logging

import requests

# Import both because the first initiates beginning in the config
import tropess_product_spec.config as tps_config
from tropess_product_spec.schema import CollectionGroup

from ..data.tool import DataTool

logger = logging.getLogger()
class DataQuery(DataTool):

    def feat_is_archived(self, feat):
        is_archived = 'archive_status' in feat['properties'] and feat['properties']['archive_status'] == "cnm_r_success"
        return is_archived

    def stac_date_status(self, stac):

        date_status = {}
        for feat in stac['features']:
            processing_date = dateparser.parse(feat['properties']['processing_datetime']).strftime("%Y-%m-%d")        
            is_archived = self.feat_is_archived(feat)
            date_info = date_status[processing_date] = date_status.get(processing_date, {})
            date_info['count'] = date_info.get('count', 0) + 1
            date_info['num_archived'] = date_info.get('num_archived', 0) + (1 if is_archived else 0)
        
        return date_status

    def get_constant_property(self, stac, prop_name, required=True):
        "Loop over metadata and ensure that all items have the same value for metadata_name"
        
        if 'features' not in stac:
            return

        prop_value = None
        for feat in stac['features']:
            if prop_name not in feat['properties']:
                print(f"{feat['id']} does not define {prop_name}")
                continue

            curr_value = feat['properties'][prop_name]
            if prop_value is not None and prop_value != curr_value:
                raise Exception(f"{prop_name} does not have a consistent value {curr_value} for {feat['id']}, expected {prop_value}")
            prop_value = curr_value

        if prop_value is None and required:
            raise Exception(f"No datasets define the {prop_name} metadata")

        return prop_value

    def data_catalog_collection_ids(self, prefix=None):
        "Retrieve collection IDs from the data catalog"
        
        catalog_collection_ids = []
        for collection_result in self.data_manager.get_collections(limit=None):
            if prefix is None or collection_result.collection_id.find(":" + prefix) >- 0:
                catalog_collection_ids.append(collection_result.collection_id)

        return catalog_collection_ids

    def data_catalog_query(self, collection_ids, processing_date, stac_output_dir=None, limit=10000):

        for curr_id in collection_ids:
            
            # Optionally output each retrieved stac catalog to a file
            stac_output_filename = None
            if stac_output_dir is not None:
                stac_output_filename = os.path.join(stac_output_dir, curr_id + ".stac")

            yield super().query_data_catalog(curr_id, processing_date, stac_output_filename=stac_output_filename)

    def display_collection_ids(self, prefix):
        
        for collection_id in self.data_catalog_collection_ids(prefix):
            logger.info(f"* {collection_id}")

    def display_collection_overview(self, collection_id, stac):
        
        table = PrettyTable(header=False)
        table.align = 'l'

        table.add_row(["Collection ID", collection_id])
        table.add_row(["Collection Group", self.get_constant_property(stac, "collection_group")])
        table.add_row(["Sensor Set", self.get_constant_property(stac, "sensor_set")])
        table.add_row(["Product Stage", self.get_constant_property(stac, "product_stage")])
        table.add_row(["Short Name", self.get_constant_property(stac, "short_name")])
        table.add_row(["Long Name", self.get_constant_property(stac, "long_name")])
        table.add_row(["Product Version", self.get_constant_property(stac, "product_version")])

        print(table)

    def display_dates(self, stac):
        
        table = PrettyTable()
        table.field_names = ["Date", "Num Species", "Num Archived"]

        dates_status = self.stac_date_status(stac)
        for processing_date in sorted(dates_status.keys()):
           table.add_row([processing_date,
                          dates_status[processing_date]['count'],
                          dates_status[processing_date]['num_archived']])

        print(table)

    def display_date_details(self, stac):

        table = PrettyTable()
        table.field_names = ["Date", "Species", "Num Files", "Is Archived"]

        for feat in stac['features']:
            table.add_row([
                dateparser.parse(feat['properties']['processing_datetime']).strftime("%Y-%m-%d"),
                feat['properties']['species'],
                len(feat['assets']),
                self.feat_is_archived(feat),
            ])

        print(table)

    def display_collection_summary(self, mdps_collection_ids, processing_date):

        for stac, collection_id in zip(self.data_catalog_query(mdps_collection_ids, processing_date), mdps_collection_ids):
            if "features" not in stac:
                continue

            if 'features' not in stac or len(stac['features']) == 0:
                print(f"{collection_id} is empty.")
                continue

            print("")
            self.display_collection_overview(collection_id, stac)

            if processing_date is None:
                self.display_dates(stac)
            else:
                self.display_date_details(stac)

    def download_collection_stac_files(self, muses_collection_ids, processing_date, stac_output_dir):

        logger.info(f"Downloading STAC catalog files to {stac_output_dir}")
        
        if not os.path.exists(stac_output_dir):
            logger.debug(f"Creating directory {stac_output_dir}")
            os.makedirs(stac_output_dir)

        _ = list(self.data_catalog_query(muses_collection_ids, processing_date, stac_output_dir))

    
    def query_data(self, collection_id_prefix, collection_id_func, collection_version, collection_group=None, processing_date=None, sensor_set_str=None, stac_output_dir=None, **kwargs):
        
        if collection_group is None:
            self.display_collection_ids(collection_id_prefix)
            return
        
        data_collection_ids = collection_id_func(collection_group, collection_version, sensor_set_str)

        if stac_output_dir is not None:
            self.download_collection_stac_files(data_collection_ids, processing_date, stac_output_dir)
        else:
            self.display_collection_summary(data_collection_ids, processing_date)
    
    def query_muses_data(self, muses_collection_version, **kwargs):
        self.query_data("MUSES", self.muses_collection_ids, muses_collection_version, **kwargs)

    def query_tropess_data(self, tropess_collection_version=None, **kwargs):

        self.query_data("TRPS", self.tropess_collection_ids, tropess_collection_version, **kwargs)
        
def main():

    parser = argparse.ArgumentParser(description="Query TROPESS data in MDPS")

    parser.add_argument("--debug", action="store_true", default=False,
        help=f"Enable verbose debug logging")
    
    parser.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=False,
        help="Keyword of the collection group representing the data being processed")
    
    parser.add_argument("-s", "--sensor_set", dest="sensor_set_str", default=None,
        help="Filter by sensor set for the collection group")

    parser.add_argument("-d", "--date", dest="processing_date", required=False,
        help="Calendar date for the MUSES data to processed into TROPESS products")

    parser.add_argument("-o", "--stac_output_dir", default=None,
        help="Location to write STAC files for each collection downloaded")

    subparsers = parser.add_subparsers(required=True, dest='subparser_name')

    # muses data
    parser_muses = subparsers.add_parser('muses',
        help=f"Query MUSES products")
    
    parser_muses.add_argument("--muses_version", dest="muses_collection_version", required=False,
        help="Collection version for the MUSES data being processed", default="1")
       
    parser_muses.set_defaults(func=DataQuery.query_muses_data)

    # py_tropess

    parser_tropess = subparsers.add_parser('tropess',
        help=f"Query TROPESS products")

    parser_tropess.add_argument( "--tropess_version", dest="tropess_collection_version", required=False,
        help="Granule version for the collection ID being delivered to the DAAC", default=2)

    parser_tropess.set_defaults(func=DataQuery.query_tropess_data)

    # final argument processing
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    args_dict = vars(args)

    # Find collection group object from keyword name
    args_dict['collection_group'] = None
    if args_dict['collection_group_keyword'] is not None:
        collection_group_obj = CollectionGroup.get_collection_group(args_dict['collection_group_keyword'])
        if collection_group_obj is None:
            raise Exception(f"Invalid collection_group_keyword: {args_dict['collection_group_keyword']}")
        args_dict['collection_group'] = collection_group_obj

    query = DataQuery(**args_dict)

    args.func(query, **args_dict)

if __name__ == "__main__":
    main()
