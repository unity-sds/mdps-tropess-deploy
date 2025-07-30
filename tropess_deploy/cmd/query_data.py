#!/usr/bin/env python3
"""
"""

import os
import re

import dateparser

import argparse

from pprint import pformat

import logging

import requests

# Import both because the first initiates beginning in the config
import tropess_product_spec.config as tps_config
from tropess_product_spec.schema import CollectionGroup

from ..data.tool import DataTool

class DataQuery(DataTool):

    def num_features(stac):
        if 'features' in stac:
            return len(stac['features'])

    def num_archived(stac):
        if not 'features' in stac:
            return 0
        
        #num_archived = math.sum([ ])
        is_archive_success = lambda feat: 'archive_status' in feat['properties'] and feat['properties']['archive_status'] == "cnm_r_success" and 1 or 0

        return sum(list(map(is_archive_success, stac['features']))) 

    def filenames(stac, ext=".nc"):
        nc_files = []
        for feat in stac['features']:
            nc_files += list(filter(lambda fn: re.search(rf'\{ext}$', fn), feat['assets'].keys()))
            
        return sorted(nc_files)

    def dates(stac, ext=".nc"):
        processing_dates = []
        for feat in stac['features']:
            meta_datetime = feat['properties']['processing_datetime']
            date_only = re.sub(r'T.*$', '', meta_datetime)
            processing_dates.append(date_only)
        return sorted(processing_dates)

    def search_url(stac):
        return stac['links'][0]['href']

    def query_input_data(self, collection_ids, processing_date, stac_output_dir=None, limit=10000):

        # Normalize date
        processing_date = dateparser.parse(processing_date).strftime("%Y-%m-%d")

        for curr_id in collection_ids:

            stac_output_filename = os

            stac_query_result = super().query_data_catalog(curr_id, processing_date, stac_output_filename=stac_output_filename)

    def muses(self, collection_group, processing_date, sensor_set_str, muses_collection_version, **kwargs):
        muses_collection_ids = self.muses_collection_ids(collection_group, muses_collection_version, sensor_set_str)

        pass

    def tropess(self):
        pass
        
def main():

    parser = argparse.ArgumentParser(description="Query TROPESS data in MDPS")

    parser.add_argument("--debug", action="store_true", default=False,
        help=f"Enable verbose debug logging")
    
    parser.add_argument("-c", "--collection_keyword", dest="collection_group_keyword", required=True,
        help="Keyword of the collection group representing the data being processed")

    parser.add_argument("-d", "--date", dest="processing_date", required=True,
        help="Calendar date for the MUSES data to processed into TROPESS products")

    parser.add_argument("-s", "--sensor_set", dest="sensor_set_str", default=None,
        help="Filter by sensor set for the collection group")

    subparsers = parser.add_subparsers(required=True, dest='subparser_name')

    # muses data
    parser_muses = subparsers.add_parser('muses',
        help=f"Query MUSES products")
    
    parser_muses.add_argument("--muses_version", dest="muses_collection_version", required=False,
        help="Collection version for the MUSES data being processed", default="1")
       
    parser_muses.set_defaults(func=DataQuery.muses)

    # py_tropess

    parser_tropess = subparsers.add_parser('py_tropess',
        help=f"Query TROPESS products")

    parser_tropess.add_argument( "--tropess_version", dest="granule_version", required=False,
        help="Granule version for the collection ID being delivered to the DAAC")

    parser_tropess.set_defaults(func=DataQuery.tropess)

    # final argument processing
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    args_dict = vars(args)

    query = DataQuery(**args_dict)

    args.func(query, **args_dict)

if __name__ == "__main__":
    main()
