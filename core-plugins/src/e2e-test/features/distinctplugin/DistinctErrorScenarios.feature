@Distinct
Feature: Distinct analytics - Verify Distinct plugin error Scenarios

  @GCS_DISTINCT_TEST1 @Distinct_Required
  Scenario:Verify Distinct plugin validation errors for incorrect data in Fields
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Distinct" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Distinct" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "gcsDistinctTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "distinctCsvAllDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Distinct"
    Then Enter the Distinct plugin fields as list "distinctInValidFieldNames"
    Then Click on the Validate button
    Then Verify Distinct plugin in-line error message for incorrect fields: "distinctInValidFieldNames"

  Scenario:Verify Distinct plugin validation errors for incorrect data in Number of Partitions
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Distinct" from the plugins list as: "Analytics"
    Then Navigate to the properties page of plugin: "Distinct"
    Then Enter input plugin property: "numberOfPartitions" with value: "distinctInvalidPartitions"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numPartitions" is displaying an in-line error message: "errorMessageDistinctInvalidNumberOfPartitions"
