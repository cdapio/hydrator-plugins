/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for GroupBy Aggregator.
 */
public class GroupByTestRun extends ETLBatchTestBase {

  @Test
  public void testGroupBy() throws Exception {

    /*
                                  |--> group by user, totalPurchases:count(*), totalSpent:sum(price) --> user table
        <ts, user, item, price> --|
                                  |--> group by item, totalPurchases:count(user), latestPurchase:max(ts) --> item table
     */
    String purchasesDatasetName = "purchases-groupbytest";
    String usersDatasetName = "users-groupbytest";
    String itemsDatasetName = "items-groupbytest";

    Schema purchaseSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    ETLStage purchaseStage =
      new ETLStage("purchases",
                   new ETLPlugin("Table",
                                 BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, purchasesDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA, purchaseSchema.toString()),
                                 null));

    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("totalPurchases", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("totalSpent", Schema.of(Schema.Type.DOUBLE)));
    ETLStage userSinkStage =
      new ETLStage("users",
                   new ETLPlugin("Table",
                                 BatchSink.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, usersDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "user",
                                   Properties.Table.PROPERTY_SCHEMA, userSchema.toString()),
                                 null));

    Schema itemSchema = Schema.recordOf(
      "item",
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("totalPurchases", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("latestPurchase", Schema.of(Schema.Type.LONG)));
    ETLStage itemSinkStage =
      new ETLStage("items",
                   new ETLPlugin("Table",
                                 BatchSink.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, itemsDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "item",
                                   Properties.Table.PROPERTY_SCHEMA, itemSchema.toString()),
                                 null));

    ETLStage userGroupStage =
      new ETLStage("userGroup",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "groupByFields", "user",
                                   "aggregates", "totalPurchases:count(*), totalSpent:sum(price)"),
                                 null));
    ETLStage itemGroupStage =
      new ETLStage("itemGroup",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "groupByFields", "item",
                                   "aggregates", "totalPurchases:count(user), latestPurchase:max(ts)"),
                                 null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(purchaseStage)
      .addStage(userSinkStage)
      .addStage(itemSinkStage)
      .addStage(userGroupStage)
      .addStage(itemGroupStage)
      .addConnection(purchaseStage.getName(), userGroupStage.getName())
      .addConnection(purchaseStage.getName(), itemGroupStage.getName())
      .addConnection(userGroupStage.getName(), userSinkStage.getName())
      .addConnection(itemGroupStage.getName(), itemSinkStage.getName())
      .build();
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "groupby-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // write input data
    // 1: 1234567890000, samuel, island, 1000000
    // 2: 1234567890001, samuel, shirt, 15.34
    // 3. 1234567890001, samuel, pie, 3.14
    // 4. 1234567890002, john, pie, 3.14
    // 5. 1234567890003, john, shirt, 20.53
    DataSetManager<Table> purchaseManager = getDataset(purchasesDatasetName);
    Table purchaseTable = purchaseManager.get();
    // 1: 1234567890000, samuel, island, 1000000
    Put put = new Put(Bytes.toBytes(1));
    put.add("ts", 1234567890000L);
    put.add("user", "samuel");
    put.add("item", "island");
    put.add("price", 1000000d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("ts", 1234567890001L);
    put.add("user", "samuel");
    put.add("item", "shirt");
    put.add("price", 15.34d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("ts", 1234567890001L);
    put.add("user", "samuel");
    put.add("item", "pie");
    put.add("price", 3.14d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("ts", 1234567890002L);
    put.add("user", "john");
    put.add("item", "pie");
    put.add("price", 3.14d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(5));
    put.add("ts", 1234567890003L);
    put.add("user", "john");
    put.add("item", "shirt");
    put.add("price", 20.53d);
    purchaseTable.put(put);
    purchaseManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> usersManager = getDataset(usersDatasetName);
    Table usersTable = usersManager.get();
    // users table should have:
    // samuel: 3, 1000000 + 15.34 + 3.14
    Row row = usersTable.get(Bytes.toBytes("samuel"));
    Assert.assertEquals(row.getLong("totalPurchases").longValue(), 3L);
    Assert.assertTrue(Math.abs(row.getDouble("totalSpent") - 1000000d - 15.34d - 3.14d) < 0.0000001);
    // john: 2, 3.14 + 20.53
    row = usersTable.get(Bytes.toBytes("john"));
    Assert.assertEquals(row.getLong("totalPurchases").longValue(), 2L);
    Assert.assertTrue(Math.abs(row.getDouble("totalSpent") - 3.14d - 20.53d) < 0.0000001);

    DataSetManager<Table> itemsManager = getDataset(itemsDatasetName);
    Table itemsTable = itemsManager.get();
    // items table should have:
    // island: 1, 1234567890000
    row = itemsTable.get(Bytes.toBytes("island"));
    Assert.assertEquals(row.getLong("totalPurchases").longValue(), 1L);
    Assert.assertEquals(row.getLong("latestPurchase").longValue(), 1234567890000L);
    // pie: 2, 1234567890002
    row = itemsTable.get(Bytes.toBytes("pie"));
    Assert.assertEquals(row.getLong("totalPurchases").longValue(), 2L);
    Assert.assertEquals(row.getLong("latestPurchase").longValue(), 1234567890002L);
    // shirt: 2, 1234567890003
    row = itemsTable.get(Bytes.toBytes("shirt"));
    Assert.assertEquals(row.getLong("totalPurchases").longValue(), 2L);
    Assert.assertEquals(row.getLong("latestPurchase").longValue(), 1234567890003L);
  }

}
