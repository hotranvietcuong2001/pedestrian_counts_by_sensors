from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
    
import operators
import helpers
import hooks

class PedestrianCountsPlugins(AirflowPlugin):
    name = "pedestrain_counts_plugins"
    operators = [
        operators.S3DeleteObjectsOperator
    ]

    helpers = [
        helpers.map_files_for_uploads,
        helpers.upload_to_s3,
    ]

    hooks = [
        hooks.S3Hook
    ]