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
package io.cdap.plugin.http.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Represents Http Plugin locators.
 */
public class HttpPluginLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-HTTP-batchsource']")
  public static WebElement httpSource;

  @FindBy(how = How.XPATH, using = "//*[contains(@class,'plugin-endpoint_HTTP')]")
  public static WebElement fromHttp;

  @FindBy(how = How.XPATH, using = "//*[@title='HTTP']//following-sibling::div")
  public static WebElement httpProperties;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='hamburgermenu-HTTP-batchsource-0-toggle']")
  public static WebElement httpSourceHamburgerMenu;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='hamburgermenu-HTTP-batchsource-0-delete']")
  public static WebElement httpSourceDelete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement httpReferenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='url' and @class='MuiInputBase-input']")
  public static WebElement httpUrl;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-httpMethod']")
  public static WebElement httpMethod;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-format']")
  public static WebElement selectFormat;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='headers']//*[@data-cy='key']/input")
  public static WebElement headersKey;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='headers']//*[@data-cy='value']/input")
  public static WebElement headersValue;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='requestBody']//textarea")
  public static WebElement requestBody;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-oauth2Enabled']")
  public static WebElement oAuthEnabled;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='authUrl' and @class='MuiInputBase-input']")
  public static WebElement authUrl;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='tokenUrl' and @class='MuiInputBase-input']")
  public static WebElement tokenUrl;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='clientId' and @class='MuiInputBase-input']")
  public static WebElement clientId;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='clientSecret' and @type='password']")
  public static WebElement clientSecret;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='refreshToken' and @class='MuiInputBase-input']")
  public static WebElement refreshToken;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='username']")
  public static WebElement basicAuthUsername;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='password']")
  public static WebElement basicAuthPassword;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='resultPath']")
  public static WebElement jsonXmlResultPath;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='fieldsMapping']//*[@data-cy='key']/input")
  public static WebElement fieldsMappingKey;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='fieldsMapping']//*[@data-cy='value']/input")
  public static WebElement fieldsMappingValue;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='plugin-properties-validate-btn']")
  public static WebElement validateButton;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-Sink-group']")
  public static WebElement sink;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-HTTP-batchsink']")
  public static WebElement httpSink;

  @FindBy(how = How.XPATH, using = "//*[@title='HTTP']")
  public static WebElement toHTTP;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='hamburgermenu-HTTP-batchsink-0-toggle']")
  public static WebElement httpSinkHamburgerMenu;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='hamburgermenu-HTTP-batchsink-0-delete']")
  public static WebElement httpSinkDelete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-method']")
  public static WebElement httpSinkMethod;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-messageFormat']")
  public static WebElement httpSinkMessageFormat;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-validation-error-msg']")
  public static WebElement httpValidationErrorMsg;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-validation-success-msg']")
  public static WebElement httpValidationSuccessMsg;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaLoadComplete;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='plugin-properties-validate-btn']/span[text()='Validate']")
  public static WebElement validateComplete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='HTTP-preview-data-btn']")
  public static WebElement httpPreviewDataButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='BigQueryTable-preview-data-btn']")
  public static WebElement bigQueryPreviewDataButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='GCSFile-preview-data-btn']")
  public static WebElement gcsPreviewDataButton;
}
