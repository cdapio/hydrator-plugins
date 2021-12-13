package io.cdap.plugin.odp.stepsdesign;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.adapter.connector.SAPAdapterImpl;
import com.google.adapter.connector.SAPProperties;
import com.google.adapter.exceptions.SystemException;
import com.google.adapter.util.ErrorCapture;
import com.google.adapter.util.ExceptionUtils;
import com.sap.conn.jco.JCoException;
import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfPipelineRunLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

/**
 * RuntimeODP.
 */
public class RuntimeODP implements CdfHelper {
  GcpClient gcpClient = new GcpClient();
  private SAPProperties sapProps;
  private ErrorCapture errorCapture;
  private SAPAdapterImpl sapAdapterImpl;
  private ExceptionUtils exceptionUtils;
  static int dsRecordsCount;
  static int countRecords;
  static int presentRecords;
  static int i = 0;
  static String number;
  static String deltaLog;
  static String rawLog;
  static PrintWriter out;
  public static CdfPipelineRunLocators cdfPipelineRunLocators =
    (CdfPipelineRunLocators) SeleniumHelper.getPropertiesLocators(CdfPipelineRunLocators.class);

  static {
    try {
      out = new PrintWriter(BeforeActions.myObj);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }


  public RuntimeODP() throws FileNotFoundException {
    number = RandomStringUtils.randomAlphanumeric(7);
  }

  @Given("Open CDF application to configure pipeline")
  public void openCDFApplicationToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is SAP ODP")
  public void sourceIsSAPODP() {
    ODPActions.selectODPSource();
  }

  @When("Target is BigQuery for ODP data transfer")
  public void targetIsBigQueryForODPDataTransfer() {
    CdfStudioActions.sinkBigQuery();
  }

  @When("Configure Direct Connection {string} {string} {string} {string} {string} {string} {string}")
  public void configureDirectConnection(String client, String sysnr, String asHost, String dsName, String gcsPath,
                                        String splitRow, String packageSize) throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterDirectConnectionProperties(client, sysnr, asHost, dsName, gcsPath, splitRow, packageSize);
  }

  @When(
    "Configure Connection {string} {string} {string} {string} {string} {string} {string} {string} {string} {string}")
  public void configureConnection(String client, String sysnr, String asHost, String dsName, String gcsPath,
                                  String splitRow, String packageSize, String msServ, String systemID,
                                  String lgrp) throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterConnectionProperties(client, sysnr, asHost, dsName, gcsPath, splitRow, packageSize,
                                         msServ, systemID, lgrp);
  }

  @When("Username and Password is provided")
  public void usernameAndPasswordIsProvided() throws IOException {
    ODPActions.enterUserNamePassword(System.getenv("AUTH_USERNAME"), System.getenv("AUTH_PASSWORD"));
  }


  @When("Run one Mode is Sync mode")
  public void runOneModeIsSyncMode() {
    ODPActions.selectSync();
  }

  @Then("Validate the Schema created")
  public void validateTheSchemaCreated() throws InterruptedException {
    ODPActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 20000);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
  }

  @Then("Close the ODP Properties")
  public void closeTheODPProperties() {
    ODPActions.closeButton();
  }

  @Then("Delete the existing Odp table {string} in bigquery")
  public void deleteTheExistingOdpTableInBigquery(String table) throws IOException, InterruptedException {
    gcpClient.dropBqQuery(CDAPUtils.getPluginProp(table));
    BeforeActions.scenario.write("Table Deleted Successfully");
  }

  @Then("link source and target")
  public void linkSourceAndTarget() {
    SeleniumHelper.dragAndDrop(ODPLocators.fromODP, CdfStudioLocators.toBigQiery);
  }

  @Then("Delete all existing records in datasource")
  public void deleteAllExistingRecordsInDatasource() throws JCoException, IOException {
    Properties connection = readPropertyODP();
    sapProps = SAPProperties.getDefault(connection);
    errorCapture = new ErrorCapture(exceptionUtils);
    sapAdapterImpl = new SAPAdapterImpl(errorCapture, connection);
    HashMap<String, String> opProps = new HashMap<>();
    opProps.put("RFC", "ZTEST_DATA_DEL_ACDOCA");
    opProps.put("autoCommit", "true");
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode objectNode = mapper.createObjectNode();
      objectNode.put("I_ALL", "X");
      JsonNode response = sapAdapterImpl.executeRFC(objectNode.toString(), opProps, "", "");
      System.out.println(response);

    } catch (Exception e) {
      throw SystemException.throwException(e.getMessage(), e);
    }

  }

  public Properties readPropertyODP() throws IOException {
    Properties prop = new Properties();
    InputStream input;
    Properties connection = new SAPProperties();

    input = new FileInputStream("src/e2e-test/resources/Google_SAP_Connection.properties");
    prop.load(input);
    Set<String> propertyNames = prop.stringPropertyNames();
    for (String property : propertyNames) {
      connection.put(property, prop.getProperty(property));
    }

    return connection;
  }


  public static void main(String args[]) throws IOException, JCoException, InterruptedException {

    RuntimeODP runtimeODP = new RuntimeODP();
    //e2EPocODP.deleteAllExistingRecordsInDatasource();
    runtimeODP.deleteTheExistingOdpTableInBigquery("tab_src01");
  }

  @Then("Create the {string} records with {string} in the ODP datasource from JCO")
  public void createTheRecordsInTheODPDatasourceFromJCO(String recordcount, String rfcName)
    throws IOException, JCoException {
    dsRecordsCount = Integer.parseInt(recordcount);
    presentRecords = presentRecords + dsRecordsCount;
    Properties connection = readPropertyODP();
    sapProps = SAPProperties.getDefault(connection);
    errorCapture = new ErrorCapture(exceptionUtils);
    sapAdapterImpl = new SAPAdapterImpl(errorCapture, connection);
    HashMap<String, String> opProps = new HashMap<>();
    opProps.put("RFC", CDAPUtils.getPluginProp(rfcName));
    opProps.put("autoCommit", "true");
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode objectNode = mapper.createObjectNode();
      objectNode.put("I_NUM", recordcount);
      JsonNode response = sapAdapterImpl.executeRFC(objectNode.toString(), opProps, "", "");
      String records = response.toString().replaceAll("\\D", "");
      BeforeActions.scenario.write("No of records created:-" + records);
      Assert.assertEquals(records, recordcount);
      Thread.sleep(60000);
    } catch (Exception e) {
      throw SystemException.throwException(e.getMessage(), e);
    }
  }

  @Then("Enter the BigQuery Properties for ODP datasource {string}")
  public void enterTheBigQueryPropertiesForODPDatasource(String tableName) throws IOException, InterruptedException {
    CdfStudioLocators.bigQueryProperties.click();
    CdfBigQueryPropertiesActions cdfBigQueryPropertiesActions = new CdfBigQueryPropertiesActions();
    SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.projectID,
                                       CDAPUtils.getPluginProp("ODP-Project-ID"));
    CdfBigQueryPropertiesLocators.bigQueryReferenceName.sendKeys("automation_test");
    CdfBigQueryPropertiesLocators.dataSetProjectID.sendKeys(CDAPUtils.getPluginProp("ODP-Project-ID"));
    CdfBigQueryPropertiesLocators.dataSet.sendKeys(CDAPUtils.getPluginProp("data-Set-ODP"));
    CdfBigQueryPropertiesLocators.bigQueryTable.sendKeys(CDAPUtils.getPluginProp(tableName));
    CdfBigQueryPropertiesLocators.validateBttn.click();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, 1L);
  }


  @Then("Close the BQ Properties")
  public void closeTheBQProperties() {
    CdfGcsActions.closeButton();
  }


  @Then("Run the ODP Pipeline in Runtime")
  public void runTheODPPipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till ODP pipeline is in running state")
  public void waitTillODPPipelineIsInRunningState() {
    Boolean bool = true;
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Open Logs of ODP Pipeline")
  public void openLogsOfODPPipeline() throws InterruptedException, FileNotFoundException {
    CdfPipelineRunAction.logsClick();
    Thread.sleep(5000);
    rawLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(rawLog);
    out.println(rawLog);
    out.close();
  }

  @Then("Verify the ODP pipeline status is {string}")
  public void verifyTheODPPipelineStatusIs(String status) {
    boolean webelement = false;
    webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("validate successMessage is displayed for the ODP pipeline")
  public void validateSuccessMessageIsDisplayedForTheODPPipeline() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Get Count of no of records transferred from ODP to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredFromODPToBigQueryIn(String table)
    throws IOException, InterruptedException {

    countRecords = gcpClient.countBqQuery(CDAPUtils.getPluginProp(table));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    if (i == 0) {
      presentRecords = presentRecords + countRecords;
    }
    i++;
  }

  @Then("Verify the Delta load transfer is successful")
  public void verifyTheDeltaLoadTransferIsSuccessfull() {
    Assert.assertEquals(presentRecords, countRecords);
  }

  @Then("Close the log window")
  public void closeTheLogWindow() {
    SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy=\"log-viewer-close-btn\"]")).click();
  }

  @Then("delete table {string} in BQ if not empty")
  public void deleteTableInBQIfNotEmpty(String table) throws IOException, InterruptedException {
    try {
      int existingRecords = gcpClient.countBqQuery(CDAPUtils.getPluginProp(table));
      if (existingRecords > 0) {
        gcpClient.dropBqQuery(CDAPUtils.getPluginProp(table));
        BeforeActions.scenario.write("Table Deleted Successfully");
      }
    } catch (Exception e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  @Then("Wait till ODP pipeline is successful again")
  public void waitTillODPPipelineIsSuccessfulAgain() {
    SeleniumHelper.isElementPresent(SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='Running']")));
    Boolean bool = true;
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    SeleniumDriver.getDriver().get(SeleniumDriver.getDriver().getCurrentUrl());
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the full load transfer is successful")
  public void verifyTheFullLoadTransferIssuccessful() {
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Open Logs of ODP Pipeline to capture delta logs")
  public void openLogsOfODPPipelineToCaptureDeltaLogs() throws InterruptedException, FileNotFoundException {
    PrintWriter out = new PrintWriter(BeforeActions.myObj);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 10000);
    wait.until(ExpectedConditions.refreshed(ExpectedConditions.visibilityOfElementLocated(By.
      xpath("//*[@class=\"run-logs-btn\"]"))));
    CdfPipelineRunAction.logsClick();
    wait.until(ExpectedConditions.refreshed(ExpectedConditions.visibilityOfElementLocated(By.
      xpath("(//*[@type=\"button\"])[7]"))));
    deltaLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(deltaLog);
    out.println(rawLog.concat("********************+delta" + deltaLog));
    out.close();
  }

  @When("Subscriber is entered")
  public void subscriberIsEntered() {
    ODPLocators.subsName.sendKeys(number);
  }

  @Then("Save and Deploy ODP Pipeline")
  public void saveAndDeployODPPipeline() throws InterruptedException {
    saveAndDeployPipeline();
  }

  @Then("Reset the parameters")
  public void resetTheParameters() {
    countRecords = 0;
    presentRecords = 0;
    dsRecordsCount = 0;
    i = 0;
    BeforeActions.scenario.write("countRecords=0;\n" +
                                   "    presentRecords=0;\n" +
                                   "    dsRecordsCount=0;");
  }
}
