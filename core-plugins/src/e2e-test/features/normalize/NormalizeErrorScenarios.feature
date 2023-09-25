@Normalize
Feature: Normalize - Verify Normalize plugin error Scenarios

  @Normalize_Required
  Scenario:Verify Normalize plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Transpose" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "Transpose"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | fieldMapping      |
      | fieldNormalizing  |

  Scenario:Verify Normalize plugin validation errors for mandatory output schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Transpose" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "Transpose"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageOutputSchema" on the header

  Scenario:Verify Normalize plugin validation errors for invalid output schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Transpose" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "Transpose"
    Then Enter Normalize plugin Fields to be Mapped "normalizeGcsInvalidFieldsMapping"
    Then Enter Normalize plugin Fields to be Normalized "normalizeInvalidFieldsToBeNormalizedGCS"
    Then Enter Normalize plugin outputSchema "normalizeGcsInvalidOutputSchema"
    Then Click on the Validate button
    Then Delete Normalize plugin outputSchema row 1
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidOutputSchema" on the header

  Scenario:Verify Normalize plugin validation errors for incorrect data in field mapping
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Transpose" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "Transpose"
    Then Enter Normalize plugin Fields to be Mapped "normalizeGcsInvalidFieldsMapping"
    Then Enter Normalize plugin Fields to be Normalized "normalizeInvalidFieldsToBeNormalizedGCS"
    Then Enter Normalize plugin outputSchema "normalizeGcsInvalidOutputSchema"
    Then Click on the Validate button
    Then Verify Normalize plugin in-line error message for incorrect field mapping: "normalizeMappingField"

  @PLUGIN-1228
  Scenario:Verify Normalize plugin validation errors for incorrect data in Normalize fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Transpose" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "Transpose"
    Then Enter Normalize plugin Fields to be Mapped "normalizeGcsInvalidFieldsMapping"
    Then Enter Normalize plugin Fields to be Normalized "normalizeInvalidFieldsToBeNormalizedGCS"
    Then Enter Normalize plugin outputSchema "normalizeGcsInvalidOutputSchema"
    Then Click on the Validate button
    Then Verify Normalize plugin in-line error message for incorrect field to normalize: "normalizeAttributeType"
