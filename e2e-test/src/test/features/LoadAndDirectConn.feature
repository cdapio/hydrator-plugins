Feature: Optional Properties

  @LOADnDIR
  Scenario:Load And Direct Connection EventHandler Trial
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Connection "S4client" "S4sysnr" "S4asHost" "allTypeDsName" "S4gcsPath" "S4Splitrow" "S4pkgSize" "load.S4msServ" "load.S4systemID" "load.S4Lgrp"
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
    Then Verify the Delta load transfer is successfull
