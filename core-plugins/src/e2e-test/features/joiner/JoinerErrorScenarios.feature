@Joiner
Feature: Joiner - Verify Joiner Plugin Error scenarios

  Scenario:Verify Joiner plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Navigate to the properties page of plugin: "Joiner"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | selectedFields |

  @JOINER_TEST2
  Scenario:Verify Joiner plugin validation errors for incorrect data in Number of Partitions
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Joiner"
    Then Enter input plugin property: "numPartitions" with value: "joinerInvalidPartitions"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "numPartitions" is displaying an in-line error message: "errorMessageJoinerInvalidNumberOfPartitions"

  @JOINER_TEST2
  Scenario:Verify Joiner plugin validation errors for basic join condition
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Joiner"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "joinKeys" is displaying an in-line error message: "errorMessageJoinerBasicJoinCondition"

  @JOINER_TEST2
  Scenario:Verify Joiner plugin validation errors for advanced join condition
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Joiner"
    Then Select radio button plugin property: "conditionType" with value: "advanced"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "conditionExpression" is displaying an in-line error message: "errorMessageJoinerAdvancedJoinCondition"

  @JOINER_TEST2
  Scenario:Verify Joiner plugin validation errors for advanced join condition with single input source
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Joiner"
    Then Select radio button plugin property: "conditionType" with value: "advanced"
    Then Enter textarea plugin property: "conditionExpression" with value: "joinConditionSQLExpression"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "conditionType" is displaying an in-line error message: "errorMessageJoinerAdvancedJoinConditionType"

  @JOINER_TEST2 @JOINER_TEST1
  Scenario:Verify Joiner plugin validation errors for input load memory field
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Connect plugins: "File2" and "Joiner" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Joiner" and "File3" to establish connection
    Then Click plugin property: "alignPlugins" button
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Select radio button plugin property: "conditionType" with value: "advanced"
    Then Enter textarea plugin property: "conditionExpression" with value: "joinConditionSQLExpression"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "inMemoryInputs" is displaying an in-line error message: "errorMessageJoinerInputLoadMemory"
