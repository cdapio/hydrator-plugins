Feature: Verification of File to BQ successful data transfer

  @File
  Scenario: To verify data is getting transferred from File to BigQuery with Mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileCsvFilePath" and format "fileCSVFileFormat"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for File connector
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Delete the table "fileBqTableName"

  @File
  Scenario: To verify successful data transfer from File to BigQuery with outputField
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileOutputFieldTestFilePath" and format "fileCSVFileFormat" with Path Field "filePathField"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Verify output field "filePathField" in target BigQuery table "fileBqTableName" contains source file path "fileOutputFieldTestFilePath"
    Then Delete the table "fileBqTableName"

  @File
  Scenario: To verify Successful File to BigQuery data transfer with Datatype override
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileCsvFilePath" and format "fileCSVFileFormat" with Override field "fileOverrideField" and data type "fileOverrideDataType"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Verify datatype of field "fileOverrideField" is overridden to data type "fileOverrideDataType" in target BigQuery table "fileBqTableName"
    Then Delete the table "fileBqTableName"

  @File
  Scenario: To verify Successful File to BigQuery data transfer with Delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileDelimitedFilePath" and format "fileTextFileFormat" with delimiter field "fileDelimiter"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Delete the table "fileBqTableName"

  @File
  Scenario: To verify Successful File to BigQuery data transfer with blob file by entering MaxMinSplitSize
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileBlobFilePath" and format "fileBlobFileFormat" with maxSplitSize "fileMaxSplitSize"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Delete the table "fileBqTableName"

  @File
  Scenario: To verify Successful File to BigQuery data transfer using Regex path filter
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect Source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileCsvFilePath" and format "fileCSVFileFormat" with regex path filter "fileRegexPathFilter"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "fileBqTableName"
    Then Delete the table "fileBqTableName"
