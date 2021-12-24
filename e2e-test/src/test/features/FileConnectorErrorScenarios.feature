Feature: Verify File plugin error scenarios

  @File1
  Scenario Outline:Verify File Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | format          |

  @File
  Scenario: To verify Pipeline preview gets failed for incorrect Regex-Path
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    When Target is BigQuery
    Then Connect source as "File" and sink as "BigQuery" to establish connection
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileCsvFilePath" and format "fileCSVFileFormat" with regex path filter "fileIncorrectRegexPath"
    Then Capture and validate output schema
    Then Validate File connector properties
    Then Close the File connector Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "fileBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @File
  Scenario: To verify Error message for invalid file path
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileInvalidBucketName" and format "fileCSVFileFormat"
    Then Verify get schema fails with error

  @File
  Scenario: To verify Error message for incorrect output path field value
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileOutputFieldTestFilePath" and format "fileCSVFileFormat" with Path Field "fileInvalidPathField"
    Then Verify Output Path field Error Message for incorrect path field "fileInvalidPathField"

  @File
  Scenario: To verify Error message for incorrect delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileDelimitedFilePath" and format "fileDelimitedFileFormat" with delimiter field "fileIncorrectDelimiter"
    Then Verify get schema fails with error

  @File
  Scenario: To verify Error message for incorrect override field
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "fileCsvFilePath" and format "fileCSVFileFormat" with Override field "fileInvalidOverrideField" and data type "fileOverrideDataType"
    Then Verify get schema fails with error
