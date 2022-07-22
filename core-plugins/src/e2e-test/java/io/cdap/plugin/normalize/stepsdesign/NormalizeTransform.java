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

package io.cdap.plugin.normalize.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.normalize.actions.NormalizeActions;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * Normalize Plugin related step design.
 */
public class NormalizeTransform implements CdfHelper {

  @Then("Delete Normalize plugin outputSchema row {int}")
  public void deleteNormalizePluginOutputSchemaRow(int row) {
    NormalizeActions.clickOutputSchemaDeleteRowButton(row);
  }

  @Then("Verify Normalize plugin in-line error message for incorrect field mapping: {string}")
  public void verifyNormalizePluginInLineErrorMessageForIncorrectFieldMapping(String fieldMapping) {
    NormalizeActions.verifyNormalizePluginPropertyInlineErrorMessageForRow
      ("fieldMapping", 1,
       PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_NORMALIZE_INVALID_FIELDMAPPING)
         .replace("FIELD_MAPPING", PluginPropertyUtils.pluginProp(fieldMapping)));
    NormalizeActions.verifyNormalizePluginPropertyInlineErrorMessageForColor("fieldMapping", 1);
  }

  @Then("Verify Normalize plugin in-line error message for incorrect field to normalize: {string}")
  public void verifyNormalizePluginInLineErrorMessageForIncorrectFieldToNormalize(String fieldNormalizing) {
    NormalizeActions.verifyNormalizePluginPropertyInlineErrorMessageForRow
      ("fieldNormalizing", 1,
       PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_NORMALIZE_INVALID_FIELDNORMALIZE)
         .replace("FIELD_NORMALIZE", PluginPropertyUtils.pluginProp(fieldNormalizing)));
    NormalizeActions.verifyNormalizePluginPropertyInlineErrorMessageForColor("fieldNormalizing", 1);
  }

  @Then("Select Normalize plugin output schema action: {string}")
  public void selectNormalizePluginOutputSchemaAction(String action) throws InterruptedException {
      NormalizeActions.selectOutputSchemaAction(action);
    }

  @Then("Enter Normalize plugin Fields to be Mapped {string}")
  public void enterNormalizePluginFieldsToBeMapped(String jsonFieldsMapping) {
    NormalizeActions.enterFieldsMapping(jsonFieldsMapping);
  }

  @Then("Enter Normalize plugin outputSchema {string}")
  public void enterNormalizePluginOutputSchema(String jsonOutputSchema) {
    NormalizeActions.enterOutputSchema(jsonOutputSchema);
  }

  @Then("Enter Normalize plugin Fields to be Normalized {string}")
  public void enterNormalizePluginFieldsToBeNormalized(String jsonFieldsNormalize) {
    NormalizeActions.enterFieldsToBeNormalized(jsonFieldsNormalize);
  }

  @Then("Validate OUT record count is equal to records transferred to target BigQuery table")
  public void validateOUTRecordCountIsEqualToRecordsTransferredToTargetBigQueryTable()
          throws IOException, InterruptedException, IOException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqTargetTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with target BigQuery table records count",
            CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);
  }

  @Then("Validate OUT record count of transpose is equal to IN record count of sink")
  public void validateOUTRecordCountOfTransposeIsEqualToINRecordCountOfSink() {
    Assert.assertEquals(recordOut(), NormalizeActions.getTargetRecordsCount());
  }
}
