/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.sendemail.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Send-email alert plugin Locators.
 */

public class SendEmailLocators {

  @FindBy(how = How.XPATH, using = "//div[@content-data='action' and normalize-space(text())='Send Email']")
  public static WebElement sendEmailAlert;

  @FindBy(how = How.XPATH, using = "//*[text()='No errors found.']")
  public static WebElement sendEmailPluginValidationSuccessMsg;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='recipients']//input")
  public static WebElement recipientInput;

  @FindBy(how = How.XPATH, using = "//button[@data-testid='config-apply-close']")
  public static WebElement pipelineAlertSaveButton;
}
