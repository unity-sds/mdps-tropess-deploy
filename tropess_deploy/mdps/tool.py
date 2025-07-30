import os
import logging

from dotenv import load_dotenv

from unity_sds_client.unity import Unity
from unity_sds_client.unity import UnityEnvironments

logger = logging.getLogger()

class MdpsTool(object):

    def __init__(self, env_config_file=None, **kwargs):

        # Load environment variables from a .env file
        load_dotenv(dotenv_path=env_config_file)

        self.mdps_project = os.environ.get("PROJECT", "unity")
        self.mdps_venue = os.environ.get("VENUE", "ops")
        self.mdps_env = os.environ.get("ENVIRONMENT", "PROD")

        self.unity = self.login_unity()

    def login_unity(self):
        "Initialize unity-sds-client"

        logger.info(f"Logging into Unity/MDPS with project = {self.mdps_project}, venue = {self.mdps_venue}, environment = {self.mdps_env}")

        env = UnityEnvironments[self.mdps_env]
        s = Unity(environment=env)
        s.set_project(self.mdps_project)
        s.set_venue(self.mdps_venue)

        return s