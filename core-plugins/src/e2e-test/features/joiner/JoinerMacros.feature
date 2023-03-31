@Joiner
Feature:Joiner - Verification of Joiner pipeline with File as source and File as sink using macros

  @JOINER_TEST3 @JOINER_TEST4 @FILE_SINK_TEST
  Scenario:To verify data is getting transferred from File to File successfully using Joiner plugin with macro arguments
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
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileSecondSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Click on the Macro button of Property: "selectedFields" and set the value to: "joinFields"
    Then Click on the Macro button of Property: "requiredInputs" and set the value to: "joinType"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click on the Macro button of Property: "joinKeys" and set the value to: "joinCondition"
    Then Click on the Macro button of Property: "inMemoryInputs" and set the value to: "joinInputLoadMemory"
    Then Click on the Macro button of Property: "joinNullKeys" and set the value to: "joinNullKeys"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "joinerOutputSchema"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File3"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "joinerFieldsList" for key "joinFields"
    Then Enter runtime argument value "joinerType" for key "joinType"
    Then Enter runtime argument value "joinerKeys" for key "joinCondition"
    Then Enter runtime argument value "joinerInputMemory" for key "joinInputLoadMemory"
    Then Enter runtime argument value "joinerNullKeys" for key "joinNullKeys"
    Then Enter runtime argument value "joinerOutputSchema" for key "joinerOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "joinerFieldsList" for key "joinFields"
    Then Enter runtime argument value "joinerType" for key "joinType"
    Then Enter runtime argument value "joinerKeys" for key "joinCondition"
    Then Enter runtime argument value "joinerInputMemory" for key "joinInputLoadMemory"
    Then Enter runtime argument value "joinerNullKeys" for key "joinNullKeys"
    Then Enter runtime argument value "joinerOutputSchema" for key "joinerOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of joiner is equal to IN record count of sink
    Then Validate output file generated by file sink plugin "fileSinkTargetBucket" is equal to expected output file "joinerMacroOutputFile"
