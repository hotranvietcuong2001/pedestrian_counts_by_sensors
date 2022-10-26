from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
    
import operators
import hooks

class PedestrianCountsPlugins(AirflowPlugin):
    name = "pedestrain_counts_plugins"
    operators = [
        operators.S3DeleteObjectsOperator
    ]


    hooks = [
        hooks.S3Hook
    ]