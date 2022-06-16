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
package io.cdap.plugin.joiner.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.joiner.locators.JoinerLocators;

/**
 *  Joiner Related Actions.
 */
public class JoinerActions {
  static {
    SeleniumHelper.getPropertiesLocators(JoinerLocators.class);
  }

  public static void clickFieldsExpandButton() {
    ElementHelper.clickOnElement(JoinerLocators.fieldsExpandButton);
  }

  public static void enterNumPartitions(String numPartitions) {
    ElementHelper.sendKeys(JoinerLocators.numPartitions, numPartitions);
  }

  public static void uncheckPluginFieldAliasCheckBox(String plugin, String field) {
    ElementHelper.selectCheckbox(JoinerLocators.fieldAliasCheckBox(plugin, field));
  }

}
