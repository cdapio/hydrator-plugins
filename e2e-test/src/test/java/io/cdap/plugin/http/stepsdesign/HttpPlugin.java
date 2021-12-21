/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.http.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.http.actions.HttpPluginActions;
import io.cdap.plugin.http.locators.HttpPluginLocators;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cdap.plugin.utils.HttpUtils;
import io.cdap.plugin.utils.JsonUtils;
import io.cdap.plugin.utils.KeyValue;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * HTTP Plugin steps design.
 */
public class HttpPlugin implements CdfHelper {
  List<String> propertiesSchemaColumnList = new ArrayList<>();
  int countRecords;

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is HTTP")
  public void sourceIsHTTP() {
    HttpPluginActions.clickHttpSource();
  }

  @When("Target is HTTP")
  public void targetIsHTTP() {
    HttpPluginActions.clickHttpSink();
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.sinkBigQuery();
  }

  @Then("Open HTTP Properties")
  public void openHTTPProperties() {
    HttpPluginActions.clickHttpProperties();
  }

  @Then("Link Source HTTP and Sink Bigquery to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toBigQiery);
    SeleniumHelper.dragAndDrop(HttpPluginLocators.fromHttp, CdfStudioLocators.toBigQiery);
  }

  @Then("Enter the HTTP Properties with Url {string} , method {string} and format {string}")
  public void enterTheHTTPPropertiesWithUrlMethodAndFormat(String url, String method, String format) {
    HttpPluginActions.enterReferenceName(E2ETestUtils.pluginProp("httpSrcReferenceName"));
    HttpPluginActions.enterHttpUrl(E2ETestUtils.pluginProp(url));
    HttpPluginActions.selectHttpMethodSource(E2ETestUtils.pluginProp(method));
    HttpPluginActions.selectFormat(E2ETestUtils.pluginProp(format));
  }

  @Then("Enter outputSchema {string} , jsonResultPath {string} and jsonFieldsMapping {string}")
  public void enterOutputSchemaJsonResultPathAndJsonFieldsMapping(
    String outputSchema, String jsonResultPath, String jsonFieldsMapping) {
    HttpPluginActions.enterJsonXmlResultPath(E2ETestUtils.pluginProp(jsonResultPath));
    HttpPluginActions.enterJSONXmlFieldsMapping(E2ETestUtils.pluginProp(jsonFieldsMapping));
    HttpPluginActions.enterOutputSchema(E2ETestUtils.pluginProp(outputSchema));
  }

  @Then("Enter request body {string}")
  public void enterRequestBody(String requestBody) {
    String body = E2ETestUtils.pluginProp(requestBody);
    if (!body.equalsIgnoreCase(StringUtils.EMPTY)) {
      HttpPluginActions.enterRequestBody(body);
    }
  }

  @Then("Enter basic authentication username and password")
  public void enterBasicAuthenticationUsernameAndPassword() {
    HttpPluginActions.enterBasicAuthUserName(System.getenv("HTTP_BASIC_USERNAME"));
    HttpPluginActions.enterBasicAuthPassword(System.getenv("HTTP_BASIC_PASSWORD"));
  }

  @Then("Enter the HTTP Properties with blank property {string}")
  public void enterTheHTTPPropertiesWithBlankProperty(String property) {
    if (property.equalsIgnoreCase("referenceName")) {
      HttpPluginActions.enterHttpUrl(E2ETestUtils.pluginProp("httpSrcUrl"));
    } else if (property.equalsIgnoreCase("url")) {
      HttpPluginActions.enterReferenceName(E2ETestUtils.pluginProp("httpSrcReferenceName"));
    } else {
      Assert.fail("Invalid Http property");
    }
  }

  @Then("Enter HTTP headers with blank value")
  public void enterHTTPHeadersWithBlankValue() {
    HttpPluginActions.enterHeadersKey("dummyKey");
  }

  @Then("Toggle OAuth2 Enabled to True")
  public void toggleOAuthEnabledToTrue() {
    HttpPluginActions.toggleOAuthEnabled();
  }

  @Then("Enter OAuth2 properties with blank {string}")
  public void enterOAuthPropertiesWithBlank(String property) {
    if (property.equalsIgnoreCase("authUrl")) {
      HttpPluginActions.enterTokenUrl(E2ETestUtils.pluginProp("httpTokenUrl"));
      HttpPluginActions.enterClientId(E2ETestUtils.pluginProp("httpClientId"));
      HttpPluginActions.enterClientSecret(E2ETestUtils.pluginProp("httpClientSecret"));
      HttpPluginActions.enterRefreshToken(E2ETestUtils.pluginProp("httpRefreshToken"));
    } else if (property.equalsIgnoreCase("tokenUrl")) {
      HttpPluginActions.enterAuthUrl(E2ETestUtils.pluginProp("httpAuthUrl"));
      HttpPluginActions.enterClientId(E2ETestUtils.pluginProp("httpClientId"));
      HttpPluginActions.enterClientSecret(E2ETestUtils.pluginProp("httpClientSecret"));
      HttpPluginActions.enterRefreshToken(E2ETestUtils.pluginProp("httpRefreshToken"));
    } else if (property.equalsIgnoreCase("clientId")) {
      HttpPluginActions.enterAuthUrl(E2ETestUtils.pluginProp("httpAuthUrl"));
      HttpPluginActions.enterTokenUrl(E2ETestUtils.pluginProp("httpTokenUrl"));
      HttpPluginActions.enterClientSecret(E2ETestUtils.pluginProp("httpClientSecret"));
      HttpPluginActions.enterRefreshToken(E2ETestUtils.pluginProp("httpRefreshToken"));
    } else if (property.equalsIgnoreCase("clientSecret")) {
      HttpPluginActions.enterAuthUrl(E2ETestUtils.pluginProp("httpAuthUrl"));
      HttpPluginActions.enterTokenUrl(E2ETestUtils.pluginProp("httpTokenUrl"));
      HttpPluginActions.enterClientId(E2ETestUtils.pluginProp("httpClientId"));
      HttpPluginActions.enterRefreshToken(E2ETestUtils.pluginProp("httpRefreshToken"));
    } else if (property.equalsIgnoreCase("refreshToken")) {
      HttpPluginActions.enterAuthUrl(E2ETestUtils.pluginProp("httpAuthUrl"));
      HttpPluginActions.enterTokenUrl(E2ETestUtils.pluginProp("httpTokenUrl"));
      HttpPluginActions.enterClientId(E2ETestUtils.pluginProp("httpClientId"));
      HttpPluginActions.enterClientSecret(E2ETestUtils.pluginProp("httpClientSecret"));
    } else {
      Assert.fail("Invalid Http OAuth2 property");
    }
  }

  @Then("Validate mandatory property error for {string}")
  public void validateMandatoryPropertyErrorFor(String property) {
    HttpPluginActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.validateComplete, 5L);
    E2ETestUtils.validateMandatoryPropertyError(property);
  }

  @Then("Validate mandatory property error for OAuth2 {string}")
  public void validateMandatoryPropertyErrorForOAuth(String property) {
    HttpPluginActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.validateComplete, 5L);
    HttpPluginActions.validateOAuthPropertyError(property);
  }

  @Then("Validate HTTP properties")
  public void validateHTTPProperties() {
    HttpPluginActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.validateComplete, 5L);
    HttpPluginActions.validatePluginValidationSuccess();
  }

  @Then("Verify plugin validation fails with error")
  public void verifyPluginValidationFailsWithError() {
    HttpPluginActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.validateComplete, 5L);
    HttpPluginActions.validatePluginValidationError();
  }

  @Then("Close the HTTP Properties")
  public void closeTheHTTPProperties() {
    HttpPluginActions.clickCloseButton();
  }

  @Then("Enter the BigQuery Sink Properties for table {string}")
  public void enterTheBigQueryPropertiesForTable(String tableName) throws InterruptedException, IOException {
    CdfStudioActions.clickProperties("BigQuery");
    CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    CdfStudioActions.clickValidateButton();
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Save the pipeline")
  public void saveThePipeline() {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("HTTP_" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
  }

  @Then("Preview and run the pipeline")
  public void previewAndRunThePipeline() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.preview, 5);
    CdfStudioLocators.preview.click();
    CdfStudioLocators.runButton.click();
  }

  @Then("Verify the preview of pipeline is {string}")
  public void verifyThePreviewOfPipelineIs(String previewStatus) {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 120);
    wait.until(ExpectedConditions.visibilityOf(CdfStudioLocators.statusBanner));
    Assert.assertTrue(CdfStudioLocators.statusBannerText.getText().contains(previewStatus));
    if (!previewStatus.equalsIgnoreCase("failed")) {
      wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    }
  }

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("HTTP_" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Open Logs")
  public void openLogs() throws FileNotFoundException, InterruptedException {
    CdfPipelineRunAction.logsClick();
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    Assert.assertTrue(SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']"));
  }

  @Then("Validate successMessage is displayed")
  public void validateSuccessMessageIsDisplayed() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Get Count of no of records transferred to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredToBigQueryIn(String tableName)
    throws IOException, InterruptedException {
    countRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(tableName));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Validate BigQuery records count is equal to HTTP records count with Url {string} {string} {string} {string}")
  public void validateBigQueryRecordsCountIsEqualToHTTPRecordsCountWithUrl
    (String url, String method, String body, String resultPath) throws UnsupportedEncodingException {
    Optional<String> response = HttpUtils
      .getHttpResponseBody(E2ETestUtils.pluginProp(url), E2ETestUtils.pluginProp(method),
                           new ArrayList<KeyValue>(), E2ETestUtils.pluginProp(body), "json");
    if (response.isPresent()) {
      Assert.assertEquals(JsonUtils.countJsonNodeSize(response.get(), E2ETestUtils.pluginProp(resultPath))
        , countRecords);
    } else {
      Assert.assertEquals(0, countRecords);
    }
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @Then("Enter the GCS Properties with {string} GCS bucket")
  public void enterTheGCSPropertiesWithGCSBucket(String bucket) throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat("csv");
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.getSchemaLoadComplete, 5);
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Link Source GCS and Sink HTTP to establish connection")
  public void linkSourceGCSAndSinkHTTPToEstablishConnection() {
    SeleniumHelper.waitElementIsVisible(HttpPluginLocators.toHTTP);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, HttpPluginLocators.toHTTP);
  }

  @Then("Enter the HTTP Sink Properties")
  public void enterTheHTTPSinkProperties() {
    HttpPluginActions.enterReferenceName(E2ETestUtils.pluginProp("httpSinkReferenceName"));
    HttpPluginActions.enterHttpUrl(E2ETestUtils.pluginProp("httpSinkUrl"));
    HttpPluginActions.selectHttpMethodSink(E2ETestUtils.pluginProp("httpSinkMethod"));
    HttpPluginActions.selectSinkMessageFormat(E2ETestUtils.pluginProp("httpSinkMessageFormat"));
    HttpPluginActions.clickValidateButton();
  }

  @Then("Validate OUT record count is equal to IN record count")
  public void validateOUTRecordCountIsEqualToINRecordCount() {
    Assert.assertEquals(recordOut(), recordIn());
  }

  @Then("Capture output schema")
  public void captureOutputSchema() {
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")), 10L);
    List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']"));
    for (WebElement element : propertiesOutputSchemaElements) {
      propertiesSchemaColumnList.add(element.getAttribute("value"));
    }
  }

  @Then("Click on PreviewData for GCS")
  public void clickOnPreviewDataForGcs() {
    CdfGcsActions.clickPreviewData();
  }

  @Then("Click on PreviewData for HTTP")
  public void clickOnPreviewDataForHttp() {
    HttpPluginActions.clickHttpPreviewData();
  }

  @Then("Verify Preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    List<String> previewSchemaColumnList = new ArrayList<>();
    List<WebElement> previewOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("(//h2[text()='Output Records']/parent::div/div/div/div/div)[1]//div[text()!='']"));
    for (WebElement element : previewOutputSchemaElements) {
      previewSchemaColumnList.add(element.getAttribute("title"));
    }
    Assert.assertTrue(previewSchemaColumnList.equals(propertiesSchemaColumnList));
  }

  @Then("Close the Preview")
  public void closeThePreview() {
    HttpPluginActions.clickCloseButton();
    CdfStudioActions.previewSelect();
  }

  @Then("Deploy the pipeline")
  public void deployThePipeline() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
    CdfStudioActions.pipelineDeploy();
  }
}
