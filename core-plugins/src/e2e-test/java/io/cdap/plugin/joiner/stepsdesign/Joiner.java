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
package io.cdap.plugin.joiner.stepsdesign;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.plugin.joiner.actions.JoinerActions;
import io.cucumber.java.en.Then;
/**
 *  Joiner Related Step Design.
 */
public class Joiner implements CdfHelper {
  @Then("Expand fields")
  public void expandFields() {
    JoinerActions.clickFieldsExpandButton();
  }

  @Then("Uncheck plugin {string} field {string} alias checkbox")
  public void uncheckPluginFieldAliasCheckBox(String plugin, String field) {
    JoinerActions.uncheckPluginFieldAliasCheckBox(plugin, field);
  }

  @Then("Enter numPartitions {int}")
  public void openJoinerProperties(int numPartitions) {
    JoinerActions.enterNumPartitions(String.valueOf(numPartitions));
  }

  @Then("Select joiner type {string}")
  public void selectJoinerType(String joinerType) {
    JoinerActions.selectJoinerType(joinerType);
  }

  @Then("Scroll to validation button and click")
  public void scrollToValidationButton() {
    ElementHelper.clickUsingActions(CdfPluginPropertiesLocators.validateButton);
  }
}
