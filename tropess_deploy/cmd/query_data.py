#!/usr/bin/env python3
"""
"""

import os

import dateparser
from datetime import datetime

import argparse

from prettytable import PrettyTable

import logging

import json

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

    def data_catalog_query(self, collection_ids, processing_date, date_range, query_limit):

        for curr_id in collection_ids:
            yield super().query_data_catalog(curr_id, processing_date=processing_date, date_range=date_range, limit=query_limit)

    def display_collection_ids(self, prefix):
        
        for collection_id in self.data_catalog_collection_ids(prefix):
            logger.info(f"* {collection_id}")

    def display_collection_overview(self, collection_id, stac, processing_date=None):
        
        table = PrettyTable(header=False)
        table.align = 'l'

        table.add_row(["Collection ID", collection_id])
        table.add_row(["Collection Group", self.get_constant_property(stac, "collection_group")])
        table.add_row(["Sensor Set", self.get_constant_property(stac, "sensor_set")])

        product_stage = self.get_constant_property(stac, "product_stage")
        table.add_row(["Product Stage", product_stage])

        if product_stage != "MUSES":
            table.add_row(["Product Type", self.get_constant_property(stac, "product_type")])
            table.add_row(["Short Name", self.get_constant_property(stac, "short_name")])
            table.add_row(["Long Name", self.get_constant_property(stac, "long_name")])

        table.add_row(["Product Version", self.get_constant_property(stac, "product_version")])

        if processing_date is not None:
            table.add_row(["Date", dateparser.parse(processing_date).strftime("%Y-%m-%d")])

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

    def display_date_details(self, stac, collection_id):

        table = PrettyTable()
        table.field_names = ["ID", "Species", "Num Files", "Is Archived"]

        table.align["ID"] = "l"
        table.align["Species"] = "l"

        for feat in stac['features']:
            id = feat['id'].replace(collection_id, "").lstrip(":")

            table.add_row([
                id,
                feat['properties']['species'],
                len(feat['assets']),
                self.feat_is_archived(feat),
            ])

        print(table)

    def display_collection_summary(self, mdps_collection_ids, stac_catalogs, processing_date):

        for stac, collection_id in zip(stac_catalogs, mdps_collection_ids):
            if "features" not in stac:
                continue

            if 'features' not in stac or len(stac['features']) == 0:
                print(f"{collection_id} is empty.")
                continue

            print("")
            self.display_collection_overview(collection_id, stac, processing_date)

            if processing_date is None:
                self.display_dates(stac)
            else:
                self.display_date_details(stac, collection_id)

    def write_stac_files(self, data_collection_ids, stac_catalogs, output_dir):

        for collection_id, stac in zip(data_collection_ids, stac_catalogs):
            stac_output_filename = os.path.join(output_dir, collection_id + ".stac")
            self.write_stac_catalog(stac, stac_output_filename)        

    def write_delete_message(self,  data_collection_ids, stac_catalogs, collection_version, output_dir):

        current_time_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        id_time_string = datetime.now().strftime("%Y%m%dT%H%M%S")

        for collection_id, stac in zip(data_collection_ids, stac_catalogs):
            if len(stac['features']) == 0:
                continue

            short_name = self.get_constant_property(stac, "short_name")
            
            for feat in stac['features']:
                product_id = feat['id'].replace(collection_id, "").lstrip(":")
                delete_message_id = f"delete-{product_id}-{id_time_string}"

                delete_params = {
                    "product": {
                    "files": [],
                        "name": product_id,
                    },
                    "identifier ": delete_message_id,
                    "collection": {
                        "name": short_name,
                        "version": str(collection_version),
                    },
                    "provider": "tropess_cloud",
                    "version":"1.3",
                    "submissionTime": current_time_string,
                }

                output_filename = os.path.join(output_dir, delete_message_id + ".json")
                logger.info(f"Writing delete message to: {output_filename}")

                with open(output_filename, "w") as delete_msg_file:
                    json.dump(delete_params, delete_msg_file)
    
    def query_data(self, collection_id_prefix, collection_id_func, collection_version, collection_group=None, processing_date=None, date_range=None, query_limit=None, sensor_set_str=None, write_stac_catalog=False, write_delete_message=False, output_dir=None, **kwargs):
        
        if collection_group is None:
            self.display_collection_ids(collection_id_prefix)
            return
        
        data_collection_ids = collection_id_func(collection_group, collection_version, sensor_set_str)

        stac_catalogs = list(self.data_catalog_query(data_collection_ids, processing_date, date_range, query_limit))
            
        self.display_collection_summary(data_collection_ids, stac_catalogs, processing_date)
        
        if output_dir is not None and not os.path.exists(output_dir):
            logger.debug(f"Creating directory {output_dir}")
            os.makedirs(output_dir)

        if write_stac_catalog:
            if output_dir:
                self.write_stac_files(data_collection_ids, stac_catalogs, output_dir)
            else:
                logger.warning(f"Can not write STAC catalog files because output directory was not defined")

        if write_delete_message:
            if output_dir is not None:
                self.write_delete_message(data_collection_ids, stac_catalogs, collection_version, output_dir)
            else:
                logger.warning(f"Can not write delete message files because output directory was not defined")
 
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

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-d", "--processing_date", dest="processing_date", required=False,
        help="Calendar date for the MUSES data to processed into TROPESS products")

    group.add_argument("-r", "--date_range", dest="date_range", nargs=2, required=False,
        help="Range of dates to query for an overview other than all")
    
    parser.add_argument("--write_stac_catalog", action="store_true", default=False,
        help="Write out STAC catalog files for each collection queried")

    parser.add_argument("--limit", dest="query_limit", type=int, required=False, default=1000,
        help="Limit the number of query results to avoid errors for large results")
 
    parser.add_argument("--write_delete_message", action="store_true", default=False,
        help="Generate a delete message JSON file for sending to DAAC")
    
    parser.add_argument("-o", "--output_dir", default=None,
        help="Location to write additional files optionally create")

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
