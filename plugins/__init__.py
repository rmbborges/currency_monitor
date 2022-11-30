from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

class AuxPlugins(AirflowPlugin):
    name = "aux_plugins"
    operators = [
        operators.LoadClosingReferenceData,
        operators.LoadCurrentCurrencyData
    ],
    hooks = [],
    helpers = [
        helpers.SqlQueries
    ]