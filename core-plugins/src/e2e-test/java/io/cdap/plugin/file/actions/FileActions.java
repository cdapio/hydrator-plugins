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

package io.cdap.plugin.file.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.file.locators.FileLocators;

import java.util.Map;

/**
 * File plugin related step actions.
 */

public class FileActions {

  static {
    SeleniumHelper.getPropertiesLocators(FileLocators.class);
  }

  public static void enterOverrideFieldName(String fieldName) {
    ElementHelper.sendKeys(FileLocators.overrideFieldName, fieldName);
  }

  public static void selectOverrideFieldDatatype(String dataType) {
    ElementHelper.selectDropdownOption(FileLocators.overrideFieldDatatype,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(dataType));
  }
}
