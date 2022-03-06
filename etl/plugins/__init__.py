from airflow.plugins_manager import AirflowPlugin

import operators
import queries

# Defining the plugin class
class NYSEPipeline(AirflowPlugin):
    name = "nyse_plugin"
    operators = [
        operators.CheckDataQualityOperator,
        operators.FetchAPIOperator,
        operators.MigrateS3ToRedshiftOperator,
        operators.ParseIndicatorsOperator,
        operators.ParseMetaDataOperator,
        operators.UploadS3Operator
    ]
    helpers = [
        queries
    ]
