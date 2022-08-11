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

package io.cdap.plugin.deduplicate.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.deduplicate.locators.DeduplicateLocators;
import org.openqa.selenium.Keys;
import org.openqa.selenium.interactions.Actions;

/**
 * Deduplicate Related Actions.
 */

public class DeduplicateActions implements CdfHelper {

  static {
    SeleniumHelper.getPropertiesLocators(DeduplicateLocators.class);

  }

  public static void enterDeduplicateFilterOperationFieldName(String fieldName) {
    ElementHelper.sendKeys(DeduplicateLocators.deduplicateFieldName, fieldName);
  }

  public static void selectDeduplicateFilterOperationFunction(String function) {
    ElementHelper.selectDropdownOption(DeduplicateLocators.deduplicateFunctionName, CdfPluginPropertiesLocators.
      locateDropdownListItem(function));
  }

  public static void pressEscapeKey() {
    Actions act = new Actions(SeleniumDriver.getDriver());
    act.sendKeys(Keys.ESCAPE).perform();
  }

  public static int getTargetRecordsCount() {
    String incount = DeduplicateLocators.targetRecordsCount.getText();
    return Integer.parseInt(incount.replaceAll(",", ""));
  }
}
