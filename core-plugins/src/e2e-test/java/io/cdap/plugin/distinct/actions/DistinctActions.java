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
package io.cdap.plugin.distinct.actions;

import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.distinct.locators.DistinctLocators;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;
import org.junit.Assert;

import java.util.Map;

/**
 * Distinct plugin step actions.
 */
public class DistinctActions {
  private static final Logger logger = LoggerFactory.getLogger(DistinctActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(DistinctLocators.class);
  }

  public static int getTargetRecordsCount() {
    String incount = DistinctLocators.targetRecordsCount.getText();
    return Integer.parseInt(incount.replaceAll(",", ""));
  }

  public static void verifyDistinctPluginPropertyInlineErrorMessageForRow(String propertyName, int row,
                                                                          String expectedMessage) {
    WaitHelper.waitForElementToBeDisplayed(DistinctLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementDisplayed(DistinctLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementContainsText(DistinctLocators.locatePluginPropertyInlineError(propertyName,
                                                                               row - 1), expectedMessage);
  }

  public static void verifyDistinctPluginPropertyInlineErrorMessageForColor(String propertyName, int row) {
    logger.info("Verify if plugin property: " + propertyName + "'s inline error message is shown in the expected color:"
                  + " " + "#a40403");
    String actualColor = ElementHelper.getElementColorCssProperty(DistinctLocators.
                                                       locatePluginPropertyInlineError(propertyName, row - 1));
    String expectedColor = "#a40403";
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void enterListOfFields(int row, String fields) {
    ElementHelper.sendKeys(DistinctLocators.listOfFields(row), fields);
  }

  public static void clickFieldsAddRowButton(int row) {
    ElementHelper.clickOnElement(DistinctLocators.fieldAddRowButton(row));
  }
}
