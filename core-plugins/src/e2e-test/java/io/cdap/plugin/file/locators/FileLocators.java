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
package io.cdap.plugin.file.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * File plugin Locators.
 */

public class FileLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='override']//*[@data-cy='key']/input")
  public static WebElement overrideFieldName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='override']//*[@data-cy='value']")
  public static WebElement overrideFieldDatatype;

  public static WebElement outputSchemaDataTypeDropdown(String fieldName) {
    String xpath = "//*[@data-cy='schema-fields-list']//input[@value='" + fieldName + "']/parent::div" +
      "//*[@data-cy='select-undefined']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static By outputSchemaDataTypeOption(String fieldName, String option) {
    return By.xpath("//*[@data-cy='schema-fields-list']//input[@value='" + fieldName + "']/parent::div" +
                      "//*[@data-cy='select-undefined']//*[@data-cy='option-" + option + "']");
  }

}
