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
package io.cdap.plugin.datetransform.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Then;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Optional;

/**
 * DateTransform related Stepsdesign.
 */

public class DateTransform implements CdfHelper {

  @Then("Validate dateFormat {string} of the fields {string} in target BQ table {string}")
  public void validateDateFormatOfTheFieldsInTargetBQTable(String dateFormat, String fields, String targetBQTable)
    throws IOException, InterruptedException {
    String[] fieldNames = PluginPropertyUtils.pluginProp(fields).split(",");
    for (String field : fieldNames) {
      Optional<String> result = BigQueryClient
        .getSoleQueryResult("SELECT " + field + " FROM `" + (PluginPropertyUtils.pluginProp("projectId")) + "."
                              + (PluginPropertyUtils.pluginProp("dataset")) + "."
                              + PluginPropertyUtils.pluginProp(targetBQTable) + "`");
      String dateInTargetTable = StringUtils.EMPTY;
      if (result.isPresent()) {
        dateInTargetTable = result.get();
      }
      SimpleDateFormat sdf = new SimpleDateFormat(PluginPropertyUtils.pluginProp(dateFormat));
      sdf.setLenient(false);
      try {
        sdf.parse(dateInTargetTable);
        BeforeActions.scenario.write("Date field " + field + " is formatted to " + PluginPropertyUtils.pluginProp
          (dateFormat) + " format in target table");
      } catch (ParseException e) {
        Assert.fail("Date transformation is not done for the field " + field + " in target table");
      }
    }
  }
}
