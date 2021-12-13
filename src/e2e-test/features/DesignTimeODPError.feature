Feature: User has entered all the details of the connection with below mentioned wrong parameter
  and by pressing validate button all the parameters are validated and error is thrown.

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty referenceName parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    Then "referenceName" as "" and getting "noRefName"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty client parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    Then "jco.client.client" as "" and getting "noJcoClient"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty lang parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "jco.client.lang" as "" and getting "noJcoLang"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty ashost parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "jco.client.ashost" as "" and getting "sapAppServerHostRequired"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty sysnr parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "jco.client.sysnr" as "" and getting "sapSystemNumberRequired"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty sapSourceObjName parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "sapSourceObjName" as "" and getting "noSapSource"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty user parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "jco.client.user" as "" and getting "noJcoUser"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if empty gcsPath parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then "gcsPath" as "" and getting "noGcsPath"
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if wrong client parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter "jco.client.client" as "abc" and getting "wrongJcoCLientConfig" for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if wrong lang parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter "jco.client.lang" as "Eq" and getting "wrongLogonLang" for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if wrong ashost parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter "jco.client.ashost" as "10.132.0.300" and getting "sapgatewayFailure" for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if wrong user parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter "jco.client.user" as "invalid" and getting "wrongCreds" for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted


  @DesignTime-TC-ODP-DSGN-03.02
  Scenario: User is able to view error messages if wrong gcs parameter is provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter "gcsPath" as "gs://invalid" and getting "missingBillingAc" for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario Outline: User is able to view error messages if wrong advanced options are provided
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameter <option> as <input> and getting row <errorMessage> for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted
    Examples:
      | option            | input                         | errorMessage                                                                                                                |
      |subscriberName     |subscriber-Name                |invalidSubscriberName|
      |numSplits          |-1                             |invalidSplits|
      |packageSize        |-1                             |invalidPackSize|

  @DesignTime-TC-ODP-DSGN-03.02
  Scenario Outline: User is able to view error messages if wrong filter options are given
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameters filterEqualKey as <filterOption> and its filterEqualVal as <query> and getting row <errorMessage> for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted
    Examples:
      | filterOption         | query             | errorMessage |
      |BALANCE               |"&*^%$#"           |invalidDSCondition|

  @ODP @DesignTime-TC-ODP-DSGN-03.02
  Scenario Outline: User is able to view error messages if wrong filter options are given in range
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username and Password is provided
    Then User is able to set parameters filterRangeKey as <filterOption> and its filterRangeVal as <query> and getting row <errorMessage> for wrong input
    Then User is able to validate the validate the error
    Then User is able to validate the text box is highlighted
    Examples:
      | filterOption         | query             | errorMessage |
      |BALANCE               |10 AND 15          | invalidRangeCondition|
      |RYEAR                 |10  15             | invalidDateRangeCondition|




