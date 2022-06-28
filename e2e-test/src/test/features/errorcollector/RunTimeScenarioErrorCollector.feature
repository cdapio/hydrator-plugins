@Error_Collector
Feature: Error Collector Plugin - Run time scenarios

  @BQ_SINK_TEST @BQ_SOURCE_TEST
  Scenario: Verify error collector plugin functionality with default values using BigQuery to BigQuery pipeline
    Given Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Wrangler" from the plugins list as: "Transform"
    And Expand Plugin group in the LHS plugins list: "Error Handlers and Alerts"
    And Select plugin: "Error Collector" from the plugins list as: "Error Handlers and Alerts"
    And Align plugin: "ErrorCollector" on canvas moving by x offset: 80 and y offset: 200
    And Expand Plugin group in the LHS plugins list: "Sink"
    And Select plugin: "BigQuery" from the plugins list as: "Sink"
    And Connect plugins: "BigQuery" and "Wrangler" to establish connection
    And Connect plugins: "ErrorCollector" and "BigQuery2" to establish connection
    And Connect plugin: "Wrangler" from node type: "error" endpoint with plugin: "ErrorCollector" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    And Click on the Get Schema button
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Wrangler"
    And Enter textarea plugin property: "recipe" with value: "conditionalData"
    And Validate "Wrangler" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "ErrorCollector"
    And Validate "ErrorCollector" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "BigQuery2"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqTargetTable"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Verify column: "defaultMsg" is added in target BigQuery table: "bqTargetTable"
    Then Verify column: "defaultCode" is added in target BigQuery table: "bqTargetTable"
    Then Verify column: "defaultNode" is added in target BigQuery table: "bqTargetTable"

  @BQ_SINK_TEST @BQ_SOURCE_TEST
  Scenario: Validate error collector plugin functionality by setting field values using BigQuery to BigQuery pipeline
    Given Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Wrangler" from the plugins list as: "Transform"
    And Expand Plugin group in the LHS plugins list: "Error Handlers and Alerts"
    And Select plugin: "Error Collector" from the plugins list as: "Error Handlers and Alerts"
    And Align plugin: "ErrorCollector" on canvas moving by x offset: 80 and y offset: 200
    And Expand Plugin group in the LHS plugins list: "Sink"
    And Select plugin: "BigQuery" from the plugins list as: "Sink"
    And Connect plugins: "BigQuery" and "Wrangler" to establish connection
    And Connect plugins: "ErrorCollector" and "BigQuery2" to establish connection
    And Connect plugin: "Wrangler" from node type: "error" endpoint with plugin: "ErrorCollector" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    And Click on the Get Schema button
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Wrangler"
    And Enter textarea plugin property: "recipe" with value: "conditionalData"
    And Validate "Wrangler" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "ErrorCollector"
    And Replace input plugin property: "errorCollectorErrorMessageColumnName" with value: "ecErrorMessageColumnName"
    And Replace input plugin property: "errorCollectorErrorCodeColumnName" with value: "ecErrorCodeColumnName"
    And Replace input plugin property: "errorCollectorErrorEmitterNodeName" with value: "ecErrorEmitterNodeName"
    And Validate "ErrorCollector" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "BigQuery2"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqTargetTable"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Verify column: "ecErrorMessageColumnName" is added in target BigQuery table: "bqTargetTable"
    Then Verify column: "ecErrorCodeColumnName" is added in target BigQuery table: "bqTargetTable"
    Then Verify column: "ecErrorEmitterNodeName" is added in target BigQuery table: "bqTargetTable"
