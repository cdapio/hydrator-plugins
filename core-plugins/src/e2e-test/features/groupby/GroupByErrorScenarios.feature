@GroupBy
Feature: GroupBy - Verify GroupBy Plugin Error scenarios

  Scenario:Verify GroupBy plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Group By" from the plugins list as: "Analytics"
    Then Navigate to the properties page of plugin: "Group By"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | groupByFields |
      | aggregates    |

  @GROUP_BY_TEST @FILE_SINK_TEST
  Scenario: Verify GroupBy plugin validation errors for incorrect data in Group by fields
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Group By" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Group By" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Group By" and "File2" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "groupByTest"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "groupByCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Group By"
    Then Select dropdown plugin property: "groupByFields" with option value: "groupByValidFirstField"
    Then Press ESC key to close the unique fields dropdown
    Then Select dropdown plugin property: "groupByFields" with option value: "groupByValidSecondField"
    Then Press ESC key to close the unique fields dropdown
    Then Enter GroupBy plugin Fields to be Aggregate "groupByInvalidAggregateFields"
    Then Click on the Validate button
    Then Verify GroupBy plugin in-line error message for incorrect Aggregator fields: "groupByInvalidAggregatorFieldValue"
