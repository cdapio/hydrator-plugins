Feature: Run Time E2E

  @ODP @RunTime-TC-ODP-RNTM-07-01
  Scenario:User configured Load connection parameters and Security parameters by providing values on SAP UI(ENV)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When LoadProp "S4client" "S4asHost" "S4msServ" "S4systemID" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize" "S4Lgrp"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters


  @ODP @RunTime-TC-ODP-RNTM-01(Direct)
  Scenario: Multi subscriber
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Subscriber is entered
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Create the "4" records with "RFC_2LIS_VAHDR" in the ODP datasource from JCO
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Subscriber is entered
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline to capture delta logs
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the Delta load transfer is successful
    Then Reset the parameters

  @ODP @RunTime-TC-ODP-RNTM-09-01(Direct)
  Scenario: User is able to Login and transfer delta load for 2LIS_02_HDR
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "dsHdr" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Create the "4" records with "RFC_2LIS_02_HDR" in the ODP datasource from JCO
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Close the log window
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is successful again
    Then Open Logs of ODP Pipeline to capture delta logs
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the Delta load transfer is successful
    Then Reset the parameters


  @ODP @RunTime-TC-ODP-RNTM-09-02
  Scenario: User is able to Login and configure the connection parameter to establish the direct connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Create the "4" records with "RFC_2LIS_VAHDR" in the ODP datasource from JCO
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Close the log window
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is successful again
    Then Open Logs of ODP Pipeline to capture delta logs
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters


  @ODP @RunTime-TC-ODP-RNTM-09-01-Full
  Scenario: User is able to Login and run the full load on SAP Transactional Data
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "dstransact" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters

  @ODP @RunTime-TC-ODP-RNTM-09-02-Full
  Scenario: User is able to Login and run the full load on SAP master attribute Data source
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "dsMasterAttr" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters

  @ODP @RunTime-TC-ODP-RNTM-09-04
  Scenario: User is able to Login and run the full load on SAP  attribute Data source
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    When Run one Mode is Sync mode
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters


  @ODP @RunTime-TC-ODP-RNTM-03-00
  Scenario:User configured Load connection parameters and all the supported data types are getting to BQ
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "allTypeDsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful
    Then Reset the parameters
