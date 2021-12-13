package io.cdap.plugin.odp.actions;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import org.apache.commons.lang3.StringUtils;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.support.PageFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * ODPActions.
 */
public class ODPActions {
  public static ODPLocators odpLocators = null;

  static {
    odpLocators = PageFactory.initElements(SeleniumDriver.getDriver(), ODPLocators.class);
  }


  public static void selectODPSource() {
    odpLocators.sapODPSource.click();
  }

  public static void getODPProperties() {
    odpLocators.sapODPProperties.click();
  }

  public static void enterConnectionProperties(
    String client, String sysnr, String asHost, String dsName, String gcsPath, String splitRow,
    String packSize, String msServ, String systemID, String lgrp) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    if (CDAPUtils.getPluginProp(msServ) != null) {
      /*For load connection*/
      odpLocators.loadServer.click();
      odpLocators.msHost.sendKeys(CDAPUtils.getPluginProp(asHost) != null ?
                                    CDAPUtils.getPluginProp(asHost) : StringUtils.EMPTY);
      odpLocators.portNumber.sendKeys(CDAPUtils.getPluginProp(msServ));
      odpLocators.systemID.sendKeys(CDAPUtils.getPluginProp(systemID) != null ?
                                      CDAPUtils.getPluginProp(systemID) : StringUtils.EMPTY);
      ODPLocators.logonGroup.sendKeys(CDAPUtils.getPluginProp(lgrp) != null ?
                                        CDAPUtils.getPluginProp(lgrp) : StringUtils.EMPTY);
    } else {
      /*For direct connection*/
      odpLocators.systemNumber.sendKeys(CDAPUtils.getPluginProp(sysnr));
      odpLocators.sapApplicationServerHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    }
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));
    JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    js.executeScript("window.scrollBy(0,350)", "");
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitRow));
    odpLocators.packageSize.sendKeys(CDAPUtils.getPluginProp(packSize) != null ?
                                       CDAPUtils.getPluginProp(packSize) : StringUtils.EMPTY);
  }

  public static void enterDirectConnectionProperties(String client, String sysnr, String asHost, String dsName
    , String gcsPath, String splitRow, String packSize) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    odpLocators.systemNumber.sendKeys(CDAPUtils.getPluginProp(sysnr));
    odpLocators.sapApplicationServerHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));
    JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    js.executeScript("window.scrollBy(0,350)", "");
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitRow));
    odpLocators.packageSize.sendKeys(CDAPUtils.getPluginProp(packSize) != null ?
                                       CDAPUtils.getPluginProp(packSize) : StringUtils.EMPTY);
  }

  public static void enterLoadConnectionProperties(String client, String asHost, String msServ, String systemID,
                                                   String lgrp, String dsName, String gcsPath, String splitrow,
                                                   String packageSize) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    odpLocators.loadServer.click();
    odpLocators.msHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    odpLocators.portNumber.sendKeys(CDAPUtils.getPluginProp(msServ));
    odpLocators.systemID.sendKeys(CDAPUtils.getPluginProp(systemID));
    ODPLocators.logonGroup.sendKeys(CDAPUtils.getPluginProp(lgrp));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));
    JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    js.executeScript("window.scrollBy(0,350)", "");
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitrow));
    odpLocators.packageSize.sendKeys(CDAPUtils.getPluginProp(packageSize));
  }

  public static void enterUserNamePassword(String username, String password) throws IOException {
    odpLocators.usernameCredentials.sendKeys(username);
    odpLocators.passwordCredentials.sendKeys(password);
  }

  public static void selectSync() {
    odpLocators.syncRadio.click();
  }

  public static void getSchema() {
    odpLocators.getSchemaButton.click();
  }

  public static void closeButton() {
    odpLocators.closeButton.click();
  }

  public static void clickAllMacroElements() {

    int a = odpLocators.macros.size();

    for (int i = 0; i < a - 1; i++) {
      odpLocators.macros.get(i).click();
    }
  }

  public static void clickMacroElement(int i) throws InterruptedException {
    odpLocators.macros.get(i).click();
  }
}
