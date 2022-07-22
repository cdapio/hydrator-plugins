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
package io.cdap.plugin.normalize.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfSchemaLocators;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.normalize.locators.NormalizeLocators;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Normalize plugin step actions.
 */
public class NormalizeActions {
  private static final Logger logger = LoggerFactory.getLogger(NormalizeActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(NormalizeLocators.class);
  }

  public static void selectOutputSchemaAction(String action) {
    ElementHelper.selectDropdownOption(CdfSchemaLocators.schemaActions,
            CdfPluginPropertiesLocators.locateDropdownListItem(action));
    WaitHelper.waitForElementToBeHidden(CdfSchemaLocators.schemaActionType(action));
  }

  public static void clickOutputSchemaDeleteRowButton(int row) {
    ElementHelper.clickOnElement(NormalizeLocators.outputSchemaDeleteRowButton(row - 1));
  }

  public static void enterFieldsMapping(String jsonFieldsMapping) {
    Map<String, String> fieldsMapping =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonFieldsMapping));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      ElementHelper.sendKeys(NormalizeLocators.fieldsMappingKey(index), entry.getKey());
      ElementHelper.sendKeys(NormalizeLocators.fieldsMappingValue(index), entry.getValue());
      ElementHelper.clickOnElement(NormalizeLocators.fieldsMappingAddRowButton(index));
      index++;
    }
  }

  public static void enterOutputSchema(String jsonOutputSchema) {
    Map<String, String> outputSchema =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonOutputSchema));
    int index = 0;
    for (Map.Entry<String, String> entry : outputSchema.entrySet()) {
      ElementHelper.sendKeys(NormalizeLocators.outputSchemaFieldName(index), entry.getKey());
      ElementHelper.clickOnElement(NormalizeLocators.outputSchemaDataTypeDropdown(index));
      SeleniumDriver.getDriver()
        .findElement(NormalizeLocators.outputSchemaDataTypeOption(index, entry.getValue())).click();
      ElementHelper.clickOnElement(NormalizeLocators.outputSchemaAddRowButton(index));
      index++;
    }
  }

  public static void enterFieldsToBeNormalized(String jsonFieldsToBeNormalized) {
    Map<String, String> fieldsToBeNormalized =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonFieldsToBeNormalized));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsToBeNormalized.entrySet()) {
      ElementHelper.sendKeys(NormalizeLocators.fieldNormalizingNormalizeField(index), entry.getKey().split("#")[0]);
      ElementHelper.sendKeys(NormalizeLocators.fieldNormalizingFieldName(index), entry.getKey().split("#")[1]);
      ElementHelper.sendKeys(NormalizeLocators.fieldNormalizingFieldValue(index), entry.getValue());
      ElementHelper.clickOnElement(NormalizeLocators.fieldsNormalizeAddRowButton(index));
      index++;
    }
  }

  public static int getTargetRecordsCount() {
    String incount = NormalizeLocators.targetRecordsCount.getText();
    return Integer.parseInt(incount.replaceAll(",", ""));
  }

  public static void verifyNormalizePluginPropertyInlineErrorMessageForRow(String propertyName, int row,
                                                                                  String expectedMessage) {
    WaitHelper.waitForElementToBeDisplayed(NormalizeLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementDisplayed(NormalizeLocators.locatePluginPropertyInlineError(propertyName, row - 1));
    AssertionHelper.verifyElementContainsText(NormalizeLocators.locatePluginPropertyInlineError(propertyName,
                                                                                  row - 1), expectedMessage);
  }

  public static void verifyNormalizePluginPropertyInlineErrorMessageForColor(String propertyName, int row) {
    logger.info("Verify if plugin property: " + propertyName + "'s inline error message is shown in the expected color:"
            + " " + "#a40403");
    String actualColor = ElementHelper.getElementColorCssProperty(NormalizeLocators.
            locatePluginPropertyInlineError(propertyName, row - 1));
    String expectedColor = "#a40403";
    Assert.assertEquals(expectedColor, actualColor);
  }
}
