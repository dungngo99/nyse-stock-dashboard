from airflow.plugins_manager import AirflowPlugin

import operators
import sql_queries

# Defining the plugin class
class NYSEPipeline(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.CheckDataQualityOperator,
        operators.FetchAPIOperator,
        operators.MigrateS3ToRedshiftOperator,
        operators.ParseIndicatorsOperator,
        operators.ParseMetaDataOperator,
        operators.UploadS3Operator
    ]
    helpers = [
        sql_queries
    ]
