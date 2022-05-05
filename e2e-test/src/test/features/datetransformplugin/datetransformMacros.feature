@DateTransform
Feature:DateTransform - Verification of DateTransform pipeline with BigQuery as source and target using macros

  @BQ_SINK_TEST @BQ_SOURCE_DATETRANSFORM_TEST @PLUGIN-1224
  Scenario: To verify data is getting transferred from BigQuery to BigQuery successfully with DateTransform plugin properties as macro arguments
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Date Transform" from the plugins list as: "Transform"
    Then Connect plugins: "BigQuery" and "DateTransform" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Capture the generated Output Schema
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "DateTransform"
    Then Click on the Macro button of Property: "SourceFieldName" and set the value to: "dateTransform.SourceFieldName"
    Then Click on the Macro button of Property: "SourceFieldDateFormat" and set the value to: "dateTransform.SourceFieldDateFormat"
    Then Click on the Macro button of Property: "TargetFieldName" and set the value to: "dateTransform.TargetFieldName"
    Then Click on the Macro button of Property: "TargetFieldDateFormat" and set the value to: "dateTransform.TargetFieldDateFormat"
    Then Validate "Date Transform" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "DateTransform" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "SourceFieldName" for key "dateTransform.SourceFieldName"
    Then Enter runtime argument value "SourceFieldDateFormat" for key "dateTransform.SourceFieldDateFormat"
    Then Enter runtime argument value "TargetFieldName" for key "dateTransform.TargetFieldName"
    Then Enter runtime argument value "TargetFieldDateFormat" for key "dateTransform.TargetFieldDateFormat"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on the Preview Data link on the Sink plugin node: "BigQueryTable"
    Then Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "SourceFieldName" for key "dateTransform.SourceFieldName"
    Then Enter runtime argument value "SourceFieldDateFormat" for key "dateTransform.SourceFieldDateFormat"
    Then Enter runtime argument value "TargetFieldName" for key "dateTransform.TargetFieldName"
    Then Enter runtime argument value "TargetFieldDateFormat" for key "dateTransform.TargetFieldDateFormat"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
    Then Validate dateFormat "dateTransform.TargetFieldDateFormat" of the fields "dateTransform.TargetFieldName" in target BQ table "bqTargetTable"
