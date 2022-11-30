from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers

class AuxPlugins(AirflowPlugin):
    name = "aux_plugins"
    operators = [],
    hooks = [],
    helpers = [
        helpers.SqlQueries
    ]