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
package io.cdap.plugin.http.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.http.locators.HttpPluginLocators;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cdap.plugin.utils.JsonUtils;
import io.cdap.plugin.utils.KeyValue;
import org.junit.Assert;
import org.openqa.selenium.By;

import java.util.List;

/**
 * Represents Http plugin related actions.
 */
public class HttpPluginActions {
  static {
    SeleniumHelper.getPropertiesLocators(HttpPluginLocators.class);
    SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
  }

  public static void clickHttpSource() {
    HttpPluginLocators.httpSource.click();
  }

  public static void clickHttpSink() {
    HttpPluginLocators.sink.click();
    HttpPluginLocators.httpSink.click();
  }

  public static void clickHttpSourceHamburgerMenu() {
    SeleniumHelper.waitAndClick(HttpPluginLocators.httpSourceHamburgerMenu);
  }

  public static void clickHttpSourceDeletePlugin() {
    HttpPluginLocators.httpSourceDelete.click();
  }

  public static void clickHttpSinkHamburgerMenu() {
    SeleniumHelper.waitAndClick(HttpPluginLocators.httpSinkHamburgerMenu);
  }

  public static void clickHttpSinkDeletePlugin() {
    HttpPluginLocators.httpSinkDelete.click();
  }

  public static void clickHttpProperties() {
    HttpPluginLocators.httpProperties.click();
  }

  public static void enterReferenceName(String referenceName) {
    HttpPluginLocators.httpReferenceName.sendKeys(referenceName);
  }

  public static void enterHttpUrl(String url) {
    HttpPluginLocators.httpUrl.sendKeys(url);
  }

  public static void selectHttpMethodSource(String httpMethod) {
    HttpPluginLocators.httpMethod.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + httpMethod + "']")));
  }

  public static void selectHttpMethodSink(String httpMethod) {
    HttpPluginLocators.httpSinkMethod.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + httpMethod + "']")));
  }

  public static void enterHeadersKey(String key) {
    HttpPluginLocators.headersKey.sendKeys(key);
  }

  public static void enterHeadersValue(String value) {
    HttpPluginLocators.headersValue.sendKeys(value);
  }

  public static void enterRequestBody(String requestBody) {
    HttpPluginLocators.requestBody.sendKeys(requestBody);
  }

  public static void toggleOAuthEnabled() {
    HttpPluginLocators.oAuthEnabled.click();
  }

  public static void enterAuthUrl(String authUrl) {
    HttpPluginLocators.authUrl.sendKeys(authUrl);
  }

  public static void enterTokenUrl(String tokenUrl) {
    HttpPluginLocators.tokenUrl.sendKeys(tokenUrl);
  }

  public static void enterClientId(String clientId) {
    HttpPluginLocators.clientId.sendKeys(clientId);
  }

  public static void enterClientSecret(String clientSecret) {
    HttpPluginLocators.clientSecret.sendKeys(clientSecret);
  }

  public static void enterRefreshToken(String refreshToken) {
    HttpPluginLocators.refreshToken.sendKeys(refreshToken);
  }

  public static void enterBasicAuthUserName(String userName) {
    HttpPluginLocators.basicAuthUsername.sendKeys(userName);
  }

  public static void enterBasicAuthPassword(String password) {
    HttpPluginLocators.basicAuthPassword.sendKeys(password);
  }

  public static void selectFormat(String format) {
    HttpPluginLocators.selectFormat.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + format + "']")));
  }

  public static void selectSinkMessageFormat(String format) {
    HttpPluginLocators.httpSinkMessageFormat.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + format + "']")));
  }

  public static void enterJsonXmlResultPath(String resultPath) {
    HttpPluginLocators.jsonXmlResultPath.sendKeys(resultPath);
  }

  public static void enterJSONXmlFieldsMapping(String jsonFieldsMapping) {
    List<KeyValue> keyValueList = JsonUtils.covertJsonStringToKeyValueList(jsonFieldsMapping);
    int index = 1;
    for (KeyValue keyValue : keyValueList) {
      SeleniumDriver.getDriver().findElement(
          By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='key']/input)[" + index + "]"))
        .sendKeys(keyValue.getKey());
      SeleniumDriver.getDriver().findElement(
          By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='value']/input)[" + index + "]"))
        .sendKeys(keyValue.getValue());
      SeleniumDriver.getDriver().findElement(
        By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='add-row'])[" + index + "]")).click();
      index++;
    }
  }

  public static void enterOutputSchema(String jsonOutputSchema) {
    List<KeyValue> keyValueList = JsonUtils.covertJsonStringToKeyValueList(jsonOutputSchema);
    int index = 0;
    for (KeyValue keyValue : keyValueList) {
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']//input"))
        .sendKeys(keyValue.getKey());
      SeleniumDriver.getDriver().findElement(
        By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                   "//*[@data-cy='select-undefined']")).click();
      SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
        By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                   "//*[@data-cy='select-undefined']//*[text()='" + keyValue.getValue() + "']")));
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                     "//input[@type='checkbox']"))
        .click();
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                     "//button[@data-cy='schema-field-add-button']"))
        .click();
      index++;
    }
  }

  public static void clickValidateButton() {
    HttpPluginLocators.validateButton.click();
  }

  public static void clickCloseButton() {
    HttpPluginLocators.closeButton.click();
  }

  public static void validatePluginValidationError() {
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_ERROR_FOUND_VALIDATION);
    String actualErrorMessage = HttpPluginLocators.httpValidationErrorMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  public static void validatePluginValidationSuccess() {
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
    String actualErrorMessage = HttpPluginLocators.httpValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  public static void validateOAuthPropertyError(String property) {
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_HTTP_OAUTH_VALIDATION)
      .replaceAll("PROPERTY", property);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement(property));
    String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void clickHttpPreviewData() {
    HttpPluginLocators.httpPreviewDataButton.click();
  }

}
