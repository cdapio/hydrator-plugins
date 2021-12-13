package io.cdap.plugin.odp.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;

/**
 * DesginTimeODP.
 */
public class DesginTimeODP {

  @Then("Connection is established")
  public void connectionIsEstablished() throws InterruptedException {
    CdfGcsActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 20000);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan(By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    Assert.assertEquals(true, CDAPUtils.schemaValidation());
  }

  @When("LoadProp {string} {string} {string} {string} {string} {string} {string} {string} {string}")
  public void loadConnection(String client, String asHost, String msServ, String systemID, String dsName,
                             String gcsPath, String splitRow, String pkgSize, String lgrp)
    throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterLoadConnectionProperties(
      client, asHost, msServ, systemID, lgrp, dsName, gcsPath, splitRow, pkgSize);
  }

  @When("User has selected Sap client macro to configure")
  public void userHasSelectedSapClientMacroToConfigure() {
    ODPLocators.macros.get(0).click();
    ODPLocators.sapClient.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.sapClient.sendKeys("${client}");

  }

  @Then("User is validate without any error")
  public void userIsValidateWithoutAnyError() throws InterruptedException {
    ODPLocators.validateButton.click();
    Assert.assertTrue(ODPLocators.successMessage.isDisplayed());
  }

  @When("User has selected Sap language macro to configure")
  public void userHasSelectedSapLagMacroToConfigure() {
    ODPLocators.macros.get(1).click();
    ODPLocators.language.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.language.sendKeys("${lang}");
  }

  @When("User has selected Sap server as host macro to configure")
  public void userHasSelectedSapServerAsHostMacroToConfigure() {
    ODPLocators.macros.get(2).click();
    ODPLocators.sapApplicationServerHost.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.sapApplicationServerHost.sendKeys("${host}");
  }

  @When("User has selected System Number macro to configure")
  public void userHasSelectedSystemNumberMacroToConfigure() {
    ODPLocators.macros.get(3).click();
    ODPLocators.systemNumber.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.systemNumber.sendKeys("${sysnr}");
  }

  @When("User has selected datasource macro to configure")
  public void userHasSelectedDatasourceMacroToConfigure() {
    ODPLocators.macros.get(5).click();
    ODPLocators.dataSourceName.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.dataSourceName.sendKeys("${dsname}");
  }

  @When("User has selected gcsPath macro to configure")
  public void userHasSelectedGcsPathMacroToConfigure() {
    ODPLocators.macros.get(10).click();
    ODPLocators.gcsPath.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.gcsPath.sendKeys("${gcs}");
  }

  @When("User has selected Sap msHost macro to configure")
  public void userHasSelectedSapMsHostMacroToConfigure() {
    ODPLocators.macros.get(2).click();
    ODPLocators.msHost.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.msHost.sendKeys("${msHOst}");
  }

  @When("User has selected Sap msServ macro to configure")
  public void userHasSelectedSapMsServMacroToConfigure() {
    ODPLocators.macros.get(4).click();
    ODPLocators.portNumber.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.portNumber.sendKeys("${portN0}");
  }

  @When("User has selected UserName and Password macro to configure")
  public void userHasSelectedUserNameAndPasswordMacroToConfigure() {
    ODPLocators.macros.get(10).click();
    ODPLocators.usernameCredentials.sendKeys(Keys.chord(Keys.CONTROL, "a"));
    ODPLocators.usernameCredentials.sendKeys("${userName}");
  }


  @When("data source as {string} is added")
  public void dataSourceAsIsAdded(String datasource) throws InterruptedException {

    for (int i = 0; i < 15; i++) {
      ODPLocators.dataSourceName.sendKeys(Keys.BACK_SPACE);
    }
    ODPLocators.dataSourceName.sendKeys(datasource);
    ODPLocators.validateButton.click();
    ODPLocators.successMessage.isDisplayed();
  }
}
