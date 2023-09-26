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
package io.cdap.plugin.joiner.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 *  Joiner Related Locators.
 */
public class JoinerLocators {

  public static WebElement fieldAliasCheckBox(String pluginName, String field) {
    String xpath = "//*[@data-cy='" + pluginName + "-stage-expansion-panel']" + "//*[@data-cy='" + field +
      "-field-selector-name']/..//*[@data-cy='" + field + "-field-selector-checkbox']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement requiredInputCheckbox(int value, String inputSchemaName) {
    String xpath = "//*[@type='checkbox'][@value='" + value + "-" + inputSchemaName + "']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement closeSelectedInputsPannel(String inputSchemaName) {
    String xpath = "//*[@data-cy='" + inputSchemaName + "-stage-expansion-panel']//*[contains(@class,'edgeEnd')]";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  @FindBy(how = How.XPATH, using = "//button[@data-cy='sql-selector-expand-collapse-btn']")
  public static WebElement fieldsExpandButton;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='numPartitions']")
  public static WebElement numPartitions;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='requiredInputs']//*[@title='Outer']")
  public static WebElement joinerTypeSelectDropdown;

  @FindBy(how = How.XPATH, using = "(//*[contains(@class, 'metric-records-out-label')])[3]/following-sibling::span")
  public static WebElement targetRecordsCount;
}
