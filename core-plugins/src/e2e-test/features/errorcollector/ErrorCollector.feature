#
# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@ErrorCollector
Feature: Error Collector - Verification of successful error emitted to ErrorCollector plugin.
  @GCS_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify error is emitted to ErrorCollector plugin successfully.
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "JavaScript" from the plugins list as: "Transform"
    When Expand Plugin group in the LHS plugins list: "Error Handlers and Alerts"
    When Select plugin: "Error Collector" from the plugins list as: "Error Handlers and Alerts"
    Then Move plugins: "ErrorCollector" by xOffset 200 and yOffset 200
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "File" and "JavaScript" to establish connection
    Then Connect node type: "error" of plugin: "JavaScript" to plugin: "ErrorCollector"
    Then Connect plugins: "ErrorCollector" and "File2" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "firstName"
    Then Enter input plugin property: "path" with value: "gcsSourceBucket1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "firstNameOutputSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "JavaScript"
    Then Replace textarea plugin property: "script" with value: "javascriptTransformScript"
    Then Validate "JavaScript" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "firstNameOutputSchema"
    Then Validate "JavaScript" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "ErrorCollector"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "errorCollectorOutputSchema"
    Then Validate "ErrorCollector" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "errorSink"
    Then Enter input plugin property: "path" with value: "gcsTargetBucket"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify the CSV Output File matches the Expected Output File: "errorCollectorOutput" With Expected Partitions: "expectedErrorCollectorOutputPartitions"
    Then Close the pipeline logs

  @ERROR_COLLECTOR_TEST @FILE_SINK_TEST @ErrorCollector
  Scenario: Verify error collector plugin functionality with default config using File Plugins and Wrangler
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Wrangler" from the plugins list as: "Transform"
    When Expand Plugin group in the LHS plugins list: "Error Handlers and Alerts"
    When Select plugin: "Error Collector" from the plugins list as: "Error Handlers and Alerts"
    Then Move plugins: "ErrorCollector" by xOffset 80 and yOffset 200
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "File" and "Wrangler" to establish connection
    Then Connect node type: "error" of plugin: "Wrangler" to plugin: "ErrorCollector"
    Then Connect plugins: "ErrorCollector" and "File2" to establish connection
    When Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "errorCollector1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "csvFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Wrangler"
    Then Enter textarea plugin property: "recipe" with value: "errorCollectorWranglerCondition"
    Then Select radio button plugin property: "onError" with value: "send-to-error-port"
    Then Validate "Wrangler" plugin properties
    When Close the Plugin Properties page
    When Navigate to the properties page of plugin: "ErrorCollector"
    Then Validate "ErrorCollector" plugin properties
    When Close the Plugin Properties page
    When Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "writeHeader"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate output file generated by file sink plugin "fileSinkTargetBucket" is equal to expected output file "errorCollectorDefaultConfigOutput"


  @ERROR_COLLECTOR_TEST @FILE_SINK_TEST @ErrorCollector
  Scenario: Verify error collector plugin functionality with custom config using File Plugins and Wrangler
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Wrangler" from the plugins list as: "Transform"
    When Expand Plugin group in the LHS plugins list: "Error Handlers and Alerts"
    When Select plugin: "Error Collector" from the plugins list as: "Error Handlers and Alerts"
    Then Move plugins: "ErrorCollector" by xOffset 80 and yOffset 200
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "File" and "Wrangler" to establish connection
    Then Connect node type: "error" of plugin: "Wrangler" to plugin: "ErrorCollector"
    Then Connect plugins: "ErrorCollector" and "File2" to establish connection
    When Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "errorCollector1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "csvFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Wrangler"
    Then Enter textarea plugin property: "recipe" with value: "errorCollectorWranglerCondition"
    Then Select radio button plugin property: "onError" with value: "send-to-error-port"
    Then Validate "Wrangler" plugin properties
    When Close the Plugin Properties page
    When Navigate to the properties page of plugin: "ErrorCollector"
    Then Replace input plugin property: "errorMessageColumnName" with value: "errorMessageColumnName"
    Then Replace input plugin property: "errorCodeColumnName" with value: "errorCodeColumnName"
    Then Replace input plugin property: "errorEmitterNodeName" with value: "errorEmitterNodeName"
    Then Validate "ErrorCollector" plugin properties
    When Close the Plugin Properties page
    When Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "writeHeader"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate output file generated by file sink plugin "fileSinkTargetBucket" is equal to expected output file "errorCollectorCustomConfigOutput"
