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
package io.cdap.plugin.file.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.file.actions.FileActions;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;

/**
 * File plugin related Step design.
 **/

public class FilePlugin implements CdfHelper {

  @Then("Enter File plugin property: override field name with value: {string}")
  public void enterFilePluginPropertyOverrideFieldNameWithValue(String fieldName) {
    FileActions.enterOverrideFieldName(PluginPropertyUtils.pluginProp(fieldName));
  }

  @Then("Select File plugin property: override field data type with value: {string}")
  public void selectFilePluginPropertyOverrideFieldDataTypeWithValue(String dataType) {
    FileActions.selectOverrideFieldDatatype(PluginPropertyUtils.pluginProp(dataType));
  }

  @Then("Verify file plugin in-line error message for incorrect pathField value: {string}")
  public void verifyFilePluginInLineErrorMessageForIncorrectPathField(String pathField) {
    CdfPluginPropertiesActions.verifyPluginPropertyInlineErrorMessage
      ("pathField",
       PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_FILE_INVALID_PATHFIELD)
         .replace("PATH_FIELD", PluginPropertyUtils.pluginProp(pathField)));
    CdfPluginPropertiesActions.verifyPluginPropertyInlineErrorMessageColor("pathField");
  }

}
