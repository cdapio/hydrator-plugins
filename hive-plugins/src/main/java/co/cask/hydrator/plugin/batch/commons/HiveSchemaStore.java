/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.commons;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;

/**
 * A class to read/write Hive table schema so that it can be used through various stage of ETL Pipeline.
 * TODO CDAP-4132: This class should be removed once we are able to access information of prepareRun in initialize.
 */
public class HiveSchemaStore {

  public static final String HIVE_TABLE_SCHEMA_STORE = "hiveTableSchemaStore";
  private static final Gson GSON = new Gson();

  public static void storeHiveSchema(BatchContext context, String hiveDBName, String tableName, HCatSchema hiveSchema) {
    KeyValueTable table = context.getDataset(HIVE_TABLE_SCHEMA_STORE);
    List<HCatFieldSchema> fields = hiveSchema.getFields();
    table.write(Joiner.on(":").join(hiveDBName, tableName), GSON.toJson(fields));
  }

  public static HCatSchema readHiveSchema(BatchRuntimeContext context, String hiveDBName, String tableName) {
    KeyValueTable table = context.getDataset(HIVE_TABLE_SCHEMA_STORE);
    String hiveSchema = Bytes.toString(table.read(Joiner.on(":").join(hiveDBName, tableName)));
    List<HCatFieldSchema> fields = GSON.fromJson(hiveSchema, new TypeToken<List<HCatFieldSchema>>() {
    }.getType());
    return new HCatSchema(fields);
  }
}
