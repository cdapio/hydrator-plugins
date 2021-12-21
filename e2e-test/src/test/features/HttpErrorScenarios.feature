Feature: HTTP Plugin Error scenarios

  @HTTP-TC
  Scenario Outline: TC-HTTP-ERROR-01:Verify HTTP plugin source validation error for Reference name and url mandatory field
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | url             |

  @HTTP-TC
  Scenario Outline: TC-HTTP-ERROR-02:Verify HTTP plugin source validation errors for OAuth2 mandatory properties
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcValidOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Toggle OAuth2 Enabled to True
    Then Enter OAuth2 properties with blank "<property>"
    Then Validate mandatory property error for OAuth2 "<property>"
    Examples:
      | property |
      | authUrl  |
      | tokenUrl  |
      | clientId  |
      | clientSecret  |
      | refreshToken  |

  @HTTP-TC
  Scenario: TC-HTTP-ERROR-03:Verify HTTP plugin validation error for invalid jsonPath and outputSchema mapping
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcBlankOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcInvalidJsonFieldsMapping"
    Then Verify plugin validation fails with error

  @HTTP-TC
  Scenario: TC-HTTP-ERROR-04:Verify HTTP plugin validation error for invalid jsonPath and outputSchema mapping
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcValidOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Enter HTTP headers with blank value
    Then Verify plugin validation fails with error

  @HTTP-TC
  Scenario: TC-HTTP-ERROR-05:Negative- Verify Pipeline preview is getting failed for invalid HTTP output schema
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcInvalidOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Validate HTTP properties
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @HTTP-TC
  Scenario: TC-HTTP-ERROR-06:Negative- Verify Pipeline preview is getting failed for incorrect jsonResultPath
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcValidOutputSchema" , jsonResultPath "httpSrcIncorrectResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Validate HTTP properties
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @HTTP-TC
  Scenario: TC-HTTP-ERROR-07:Negative- Verify Pipeline preview is getting failed for incorrect HTTP URL
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcIncorrectUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcValidOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Validate HTTP properties
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @HTTP-TC
  Scenario Outline: TC-HTTP-ERROR-08:Verify HTTP plugin sink validation error for Reference name and url mandatory field
    Given Open Datafusion Project to configure pipeline
    When Target is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | url             |
