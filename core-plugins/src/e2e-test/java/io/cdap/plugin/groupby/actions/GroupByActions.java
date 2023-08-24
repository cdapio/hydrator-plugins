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

package io.cdap.plugin.groupby.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.groupby.locators.GroupByLocators;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;
import org.junit.Assert;
import org.openqa.selenium.ElementClickInterceptedException;
import org.openqa.selenium.Keys;
import org.openqa.selenium.interactions.Actions;

import java.util.Map;

/**
 * GroupBy plugin related Actions.
 */

public class GroupByActions {
  private static final Logger logger = (Logger) LoggerFactory.getLogger(GroupByActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(GroupByLocators.class);
  }

  public static int getTargetRecordsCount() {
    String incount = GroupByLocators.targetRecordsCount.getText();
    return Integer.parseInt(incount.replaceAll(",", ""));
  }

  public static void enterAggregates(String jsonAggreegatesFields) {
    Map<String, String> fieldsMapping =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonAggreegatesFields));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      ElementHelper.sendKeys(GroupByLocators.field(index), entry.getKey().split("#")[0]);
      ElementHelper.clickOnElement(GroupByLocators.fieldFunction(index));
      int attempts = 0;
      while (attempts < 5) {
        try {
          ElementHelper.clickOnElement(SeleniumDriver.getDriver().findElement(CdfPluginPropertiesLocators.
                                                       locateDropdownListItem(entry.getKey().split("#")[1])));
          break;
        } catch (ElementClickInterceptedException e) {
          if (attempts == 4) {
            throw e;
          }
        }
        attempts++;
      }
      if (entry.getKey().split("#")[1].contains("If")) {
        ElementHelper.sendKeys(GroupByLocators.fieldFunctionCondition(index), entry.getKey().split("#")[2]);
      }
      ElementHelper.sendKeys(GroupByLocators.fieldFunctionAlias(index), entry.getValue());
      ElementHelper.clickOnElement(GroupByLocators.fieldAddRowButton(index));
      index++;
    }
  }

  public static void verifyGroupByPluginPropertyInlineErrorMessageForRow(String propertyName, int row,
                                                                         String expectedMessage) {
    WaitHelper.waitForElementToBeDisplayed(GroupByLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementDisplayed(GroupByLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementContainsText(GroupByLocators.locatePluginPropertyInlineError(propertyName,
                                                                                row - 1), expectedMessage);
  }

  public static void verifyGroupByPluginPropertyInlineErrorMessageForColor(String propertyName, int row) {
    logger.info("Verify if plugin property: " + propertyName + "'s inline error message is shown in the expected color:"
                  + " " + "#a40403");
    String actualColor = ElementHelper.getElementColorCssProperty(GroupByLocators.
                                                         locatePluginPropertyInlineError(propertyName, row - 1));
    String expectedColor = "#a40403";
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void pressEscapeKey() {
    Actions act = new Actions(SeleniumDriver.getDriver());
    act.sendKeys(Keys.ESCAPE).perform();
  }
}
