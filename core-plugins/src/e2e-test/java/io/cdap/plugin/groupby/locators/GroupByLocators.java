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

package io.cdap.plugin.groupby.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * GroupBy plugin related Locators.
 */

public class GroupByLocators {

  public static WebElement fieldsKey(int row) {
    String xpath = "//*[@data-cy='groupByFields']//*[@data-cy= '" + row + "']//*[@data-cy='key']/input";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldsAddRowButton(int row) {
    String xpath = "//*[@data-cy='groupByFields']//*[@data-cy='" + row + "']//button[@data-cy='add-row']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement field(int row) {
    String xpath = "//div[@data-cy='aggregates']//div[@data-cy= '" + row + "']//input[@placeholder='field']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldFunction(int row) {
    String xpath = "//div[@data-cy='aggregates']//div[@data-cy= '" + row + "']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldFunctionAlias(int row) {
    String xpath = "//div[@data-cy='aggregates']//div[@data-cy= '" + row + "']//input[@placeholder='alias']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldAddRowButton(int row) {
    String xpath = "//*[@data-cy='aggregates']//*[@data-cy='" + row + "']//button[@data-cy='add-row']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement locatePluginPropertyInlineError(String propertyName, int row) {
    return SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + propertyName + "']//div[@data-cy=" +
                                                             "'error-text-" + row + "']"));
  }

  @FindBy(how = How.XPATH, using = "(//*[contains(@class, 'metric-records-out-label')])[3]/following-sibling::span")
  public static WebElement targetRecordsCount;

  public static WebElement listOfFields(int row) {
    String xpath = "//*[@data-cy='groupByFields']//*[@data-cy= '" + row + "']//*[@data-cy='key']/input";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement addFieldsRowButton(int row) {
    String xpath = "//*[@data-cy='groupByFields']//*[@data-cy='" + row + "']//button[@data-cy='add-row']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldFunctionCondition(int row) {
    String xpath = "//div[@data-cy='aggregates']//div[@data-cy= '" + row + "']//input[@placeholder='condition']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }
}
