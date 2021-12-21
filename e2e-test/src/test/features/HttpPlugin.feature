Feature: HTTP Plugin Positive scenarios

  @HTTP-TC
  Scenario: TC-HTTP-BQ-1:User is able to Login and confirm data is getting transferred from HTTP GET request to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcValidOutputSchema" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping"
    Then Validate HTTP properties
    Then Capture output schema
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for HTTP
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate successMessage is displayed
    Then Validate OUT record count is equal to IN record count
    Then Get Count of no of records transferred to BigQuery in "httpBqTableName"
    Then Validate BigQuery records count is equal to HTTP records count with Url "httpSrcUrl" "httpSrcMethod" "" "httpSrcResultPath"

  @HTTP-TC_NeedTestURL
  Scenario: TC-HTTP-BQ-2:User is able to Login and confirm data is getting transferred from HTTP POST request to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrl_2" , method "httpSrcMethod_2" and format "httpSrcFormat_2"
    Then Enter request body "httpSrcRequestBody_2"
    Then Enter outputSchema "httpSrcValidOutputSchema_2" , jsonResultPath "httpSrcResultPath_2" and jsonFieldsMapping "httpSrcValidJsonFieldsMapping_2"
    Then Validate HTTP properties
    Then Capture output schema
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for HTTP
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate successMessage is displayed
    Then Validate OUT record count is equal to IN record count
    Then Get Count of no of records transferred to BigQuery in "httpBqTableName"
    Then Validate BigQuery records count is equal to HTTP records count with Url "httpSrcUrl_2" "httpSrcMethod_2" "httpSrcRequestBody_2" "httpSrcResultPath_2"

  @HTTP-TC_Bug
  Scenario: TC-HTTP-BQ-3:User is able to Login and confirm data is getting transferred from HTTP with basic auth to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with Url "httpSrcUrlBasicAuth" , method "httpSrcMethod" and format "httpSrcFormat"
    Then Enter outputSchema "httpSrcOutputSchemaBasicAuth" , jsonResultPath "httpSrcResultPath" and jsonFieldsMapping "httpSrcJsonFieldsMappingBasicAuth"
    Then Enter basic authentication username and password
    Then Validate HTTP properties
    Then Capture output schema
    Then Close the HTTP Properties
    Then Enter the BigQuery Sink Properties for table "httpBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for HTTP
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate successMessage is displayed
    Then Validate OUT record count is equal to IN record count
    Then Get Count of no of records transferred to BigQuery in "httpBqTableName"
