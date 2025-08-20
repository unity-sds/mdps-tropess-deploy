import json
import logging

import dateparser

from unity_sds_client.unity_services import UnityServices as services
from unity_sds_client.resources.collection import Collection

import tropess_product_spec.config as tps_config
from tropess_product_spec.config import collection_group_combinations
from tropess_product_spec.product_naming import format_short_name
from tropess_product_spec.schema import SensorSet

from ..mdps.tool import MdpsTool

logger = logging.getLogger()

class DataTool(MdpsTool):
    
    def __init__(self, env_config_file=None, **kwargs):
        super().__init__(env_config_file=env_config_file, **kwargs)

        self.data_manager = self.unity.client(services.DATA_SERVICE)
    
    def collection_group_short_names(self, collection_group, sensor_set=None):
        "Return all TROPESS short names, aka the DAAC collection ID for a collection group"

        short_name_list = []

        sensor_set_filter = None
        if sensor_set is not None:
            sensor_set_filter = [sensor_set.alias]

        # Create TROPESS shortname list
        for group_kw, product_kw, sensor_set_kw, species_kw in collection_group_combinations(collection_groups_filter=[collection_group.keyword], sensor_sets_filter=sensor_set_filter):
            short_name = format_short_name(group_kw, product_kw, sensor_set_kw, species_kw)
            short_name_list.append(short_name)

        return short_name_list

    def muses_short_names(self, collection_group, sensor_set=None):
        "Return all TROPESS short names, aka the DAAC collection ID for a collection group"

        short_name_list = []

        if sensor_set is not None:
            sensor_set_list = [ sensor_set ]
        else:
            sensor_set_list = collection_group.sensor_sets.values()

        # Create MUSES shortname list
        for sensor_set in sensor_set_list:
            short_name = f'MUSES-{sensor_set.short_name}-{collection_group.short_name}'
            short_name_list.append(short_name)

        return short_name_list

    def mdps_collection_ids(self, tropess_short_names, collection_version):
        "Generate MDPS collection IDs from TROPESS short names"
    
        # Create a MDPS/Unity collection for each TROPESS product shortname in the collection group
        our_collection_ids = []
        for short_name in tropess_short_names:
            collection_id = f"URN:NASA:UNITY:{self.mdps_project}:{self.mdps_venue}:" + f"{short_name}___{collection_version}"
            our_collection_ids.append(collection_id)
 
        return our_collection_ids

    def _find_sensor_set(self, collection_group_obj, sensor_set_query):
        "Find a sensor set string via straight keyword or from an alias attached to the collection_group"

        if sensor_set_query is None:
            return None
        
        if isinstance(sensor_set_query, SensorSet):
            return sensor_set_query

        # Sensor Set object from keyword
        sensor_set_obj = tps_config.sensor_sets.get(sensor_set_query, None)
        
        # A collection group has a set of sensor sets that are valid, the mappings are mapped by the string used for the directory structure
        # Try the string with this alias
        if sensor_set_obj is None:
            sensor_set_mapping = collection_group_obj.sensor_set_mappings.get(sensor_set_query, None)
            if sensor_set_mapping is not None:
                sensor_set_obj = sensor_set_mapping.sensor_set
        
        if sensor_set_obj is None:
            raise Exception(f'Could not determine sensor set from string: "{sensor_set_query}"')

        return sensor_set_obj

    def muses_collection_ids(self, collection_group, collection_version, sensor_set_str=None):

        sensor_set_obj = self._find_sensor_set(collection_group, sensor_set_str)

        short_names = self.muses_short_names(collection_group, sensor_set=sensor_set_obj)
        return self.mdps_collection_ids(short_names, collection_version)

    def tropess_collection_ids(self, collection_group, collection_version, sensor_set_str=None):

        sensor_set_obj = self._find_sensor_set(collection_group, sensor_set_str)

        short_names = self.collection_group_short_names(collection_group, sensor_set=sensor_set_obj)
        return self.mdps_collection_ids(short_names, collection_version)

    def query_data_catalog(self, mdps_collection_id, processing_date=None, date_range=None, limit=1000):
        
        logger.debug(f"Searching data catalog for MUSES data for collection {mdps_collection_id} on date {processing_date}")

        # Get consistent date string for DS query -> YYYY-MM-DD
        if date_range is not None:
            try:
                start_date, stop_date = [ dateparser.parse(d).strftime("%Y-%m-%d") for d in date_range ]
            except AttributeError as exc:
                raise AttributeError(f"Invalid date range values: {date_range}")
            query_filter = f"processing_datetime>='{start_date}' and processing_datetime<='{stop_date}'"
        elif processing_date is not None:
            processing_date = dateparser.parse(processing_date).strftime("%Y-%m-%d")
            query_filter = f"processing_datetime='{processing_date}'"
        else:
            query_filter = None
            
        if query_filter is not None:
            logger.debug(f"Query filter: {query_filter}")

        stac_query_result = self.data_manager.get_collection_data(Collection(mdps_collection_id), limit=limit, filter=query_filter, output_stac=True)

        if 'features' not in stac_query_result:
            raise Exception(f"Error querying data catalog: {stac_query_result}")

        return stac_query_result

    def write_stac_catalog(self, stac_query_result, stac_output_filename):
        # Write out STAC file
        logger.info(f"Writing STAC result to: {stac_output_filename}")
        with open(stac_output_filename, "w") as stage_in_file:
            json.dump(stac_query_result, stage_in_file)