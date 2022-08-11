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

package io.cdap.plugin.deduplicate.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.deduplicate.actions.DeduplicateActions;
import io.cucumber.java.en.Then;
import org.junit.Assert;

/**
 * Deduplicate Plugin related StepsDesign.
 */
public class Deduplicate implements CdfHelper {

  @Then("Enter Deduplicate plugin property: filterOperation field name with value: {string}")
  public void enterDeduplicatePluginPropertyFilterOperationFieldNameWithValue(String fieldName) {
    DeduplicateActions.enterDeduplicateFilterOperationFieldName(PluginPropertyUtils.pluginProp(fieldName));
  }

  @Then("Press ESC key to close the unique fields dropdown")
  public void pressESCKeyToCloseTheUniqueFieldsDropdown() {
    DeduplicateActions.pressEscapeKey();
  }

  @Then("Select Deduplicate plugin property: filterOperation field function with value: {string}")
  public void selectDeduplicatePluginPropertyFilterOperationFieldFunctionWithValue(String function) {
    DeduplicateActions.selectDeduplicateFilterOperationFunction(PluginPropertyUtils.pluginProp(function));
  }

  @Then("Validate OUT record count of deduplicate is equal to IN record count of sink")
  public void validateOUTRecordCountOfDeduplicateIsEqualToINRecordCountOfSink() {
    Assert.assertEquals(recordOut(), DeduplicateActions.getTargetRecordsCount());
  }
}
