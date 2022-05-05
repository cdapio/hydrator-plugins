@DateTransform
Feature:DateTransform - Verify DateTransform Plugin Error scenarios

  @BQ_SOURCE_DATETRANSFORM_TEST
  Scenario:Verify DateTransform plugin validation errors for mandatory fields
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
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | sourceField |
      | targetField |

  @BQ_SOURCE_DATETRANSFORM_TEST
  Scenario:Verify DateTransform plugin error for invalid Source Field Name
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
    Then Enter input plugin property: "sourceFieldName" with value: "dateTransform.IncorrectFieldName"
    Then Enter input plugin property: "targetFieldName" with value: "dateTransform.TargetFieldName"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageDateTransformInvalidSourceFieldName" on the header

  @BQ_SOURCE_DATETRANSFORM_TEST
  Scenario:Verify DateTransform plugin error for invalid Target Field Name
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
    Then Enter input plugin property: "sourceFieldName" with value: "dateTransform.SourceFieldName"
    Then Enter input plugin property: "targetFieldName" with value: "dateTransform.IncorrectFieldName"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageDateTransformInvalidTargetFieldName" on the header

  @BQ_SOURCE_DATETRANSFORM_TEST
  Scenario:Verify DateTransform plugin error for Source and Target field must have same number of fields
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
    Then Enter input plugin property: "sourceFieldName" with value: "dateTransform.SourceFieldNames"
    Then Enter input plugin property: "targetFieldName" with value: "dateTransform.TargetFieldName"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageDateTransformMustHaveSameNumberOfFields" on the header

  @BQ_SOURCE_DATETRANSFORM_TEST
  Scenario:Verify DateTransform plugin error for No Input Schema available
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Date Transform" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "DateTransform"
    Then Enter input plugin property: "sourceFieldName" with value: "dateTransform.SourceFieldName"
    Then Enter input plugin property: "targetFieldName" with value: "dateTransform.TargetFieldName"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageDateTransformForInputSchema" on the header
