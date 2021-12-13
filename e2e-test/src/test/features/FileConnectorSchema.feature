Feature: Validate File plugin output schema for different formats
  @File
  Scenario Outline:FILE Source output schema validation
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "<Bucket>" and format "<FileFormat>"
    Then Capture and validate output schema
    Examples:
      | Bucket                    | FileFormat             |
      | fileCsvFilePath           | fileCSVFileFormat      |
      | fileTsvFilePath           | fileTSVFileFormat      |
      | fileBlobFilePath          | fileBlobFileFormat     |

  @File
  Scenario Outline:FILE Source output schema validation for delimited files
    Given Open Datafusion Project to configure pipeline
    When Source is File connector
    Then Open File connector properties
    Then Enter the File connector Properties with file path "<Bucket>" and format "<FileFormat>" with delimiter field "<Delimiter>"
    Then Capture and validate output schema
    Examples:
      | Bucket                    | FileFormat              | Delimiter     |
      | fileDelimitedFilePath     | fileDelimitedFileFormat | fileDelimiter  |
      | fileTextFilePath          | fileTextFileFormat      | fileDelimiter  |
