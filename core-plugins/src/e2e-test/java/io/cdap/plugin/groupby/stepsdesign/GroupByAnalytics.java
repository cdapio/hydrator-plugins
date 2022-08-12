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

package io.cdap.plugin.groupby.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.groupby.actions.GroupByActions;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.junit.Assert;

/**
 * GroupBy plugin related StepsDesign.
 */

public class GroupByAnalytics implements CdfHelper {

  @Then("Validate OUT record count of groupby is equal to IN record count of sink")
  public void validateOUTRecordCountOfGroupByIsEqualToINRecordCountOfSink() {
    Assert.assertEquals(recordOut(), GroupByActions.getTargetRecordsCount());
  }

  @Then("Enter GroupBy plugin Fields to be Aggregate {string}")
  public void enterGroupByPluginFieldsToBeAggregate(String jsonAggregateField) {
    GroupByActions.enterAggregates(jsonAggregateField);
  }

  @Then("Verify GroupBy plugin in-line error message for incorrect GroupBy fields: {string}")
  public void verifyGroupByPluginInLineErrorMessageForIncorrectGroupByFields(String fields) {
      GroupByActions.verifyGroupByPluginPropertyInlineErrorMessageForRow
        ("groupByFields", 1,
         PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GROUPBY_INVALID_FIELDS)
           .replace("FIELDS", PluginPropertyUtils.pluginProp(fields)));
      GroupByActions.verifyGroupByPluginPropertyInlineErrorMessageForColor("groupByFields", 1);
  }

  @Then("Verify GroupBy plugin in-line error message for incorrect Aggregator fields: {string}")
  public void verifyGroupByPluginInLineErrorMessageForIncorrectAggregatorFields(String fields) {
    GroupByActions.verifyGroupByPluginPropertyInlineErrorMessageForRow
      ("aggregates", 1,
       PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GROUPBY_AGGREGATOR_INVALID_FIELD)
         .replace("FIELDS", PluginPropertyUtils.pluginProp(fields)));
    GroupByActions.verifyGroupByPluginPropertyInlineErrorMessageForColor("aggregates", 1);
  }

  @Then("Press ESC key to close the unique fields dropdown")
  public void pressESCKeyToCloseTheUniqueFieldsDropdown() {
      GroupByActions.pressEscapeKey();
  }
}
