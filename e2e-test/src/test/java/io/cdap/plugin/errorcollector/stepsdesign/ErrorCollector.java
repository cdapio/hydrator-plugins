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
package io.cdap.plugin.errorcollector.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.Optional;

/**
 * Error Collector plugin related step design.
 */

public class ErrorCollector {

  @Then("Verify column: {string} is added in target BigQuery table: {string}")
  public void verifyColumnNameFromBigQueryTargetTable(String column, String tableName) throws IOException,
    InterruptedException {
    String columnName = PluginPropertyUtils.pluginProp(column);
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT column_name FROM `" + (PluginPropertyUtils.pluginProp("projectId")) + "."
                            + (PluginPropertyUtils.pluginProp("dataset")) + ".INFORMATION_SCHEMA.COLUMNS` " +
                            "WHERE table_name = '" + PluginPropertyUtils.pluginProp(tableName)
                            + "' and column_name = '" + columnName + "' ");
    String targetTableColumnName = StringUtils.EMPTY;
    if (result.isPresent()) {
      targetTableColumnName = result.get();
    }
    Assert.assertTrue("Column '" + columnName + "' should present in target BigQuery table",
                      targetTableColumnName.equalsIgnoreCase(columnName));
  }
}
