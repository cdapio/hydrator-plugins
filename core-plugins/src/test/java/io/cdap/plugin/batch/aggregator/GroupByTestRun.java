/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.common.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for GroupBy Aggregator.
 */
public class GroupByTestRun extends ETLBatchTestBase {

  @Test
  public void testGroupBy() throws Exception {
    testGroupBy(Engine.SPARK);
    testGroupBy(Engine.MAPREDUCE);
  }

  private void testGroupBy(Engine engine) throws Exception {

    /*
                                  |--> group by user, totalPurchases:count(*), totalSpent:sum(price) --> user table
        <ts, user, item, price> --|
                                  |--> group by item, totalPurchases:count(user), latestPurchase:max(ts) --> item table
                                  |
                                  |  test same name can be used in the output schema
                                  |--> group by user, price:max(price) --> max table
     */
    String purchasesDatasetName = "purchases-groupbytest-" + engine;
    String usersDatasetName = "users-groupbytest-" + engine;
    String itemsDatasetName = "items-groupbytest-" + engine;
    String maxDatasetName = "max-groupbytest-" + engine;

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

    Schema maxSchema = Schema.recordOf(
      "max",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    ETLStage maxSinkStage =
      new ETLStage("max",
                   new ETLPlugin("Table",
                                 BatchSink.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, maxDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "user",
                                   Properties.Table.PROPERTY_SCHEMA, maxSchema.toString()),
                                 null));

    ETLStage maxGroupStage =
      new ETLStage("maxGroup",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "groupByFields", "user",
                                   "aggregates", "price:max(price)"),
                                 null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(purchaseStage)
      .addStage(userSinkStage)
      .addStage(itemSinkStage)
      .addStage(maxSinkStage)
      .addStage(userGroupStage)
      .addStage(itemGroupStage)
      .addStage(maxGroupStage)
      .addConnection(purchaseStage.getName(), userGroupStage.getName())
      .addConnection(purchaseStage.getName(), itemGroupStage.getName())
      .addConnection(purchaseStage.getName(), maxGroupStage.getName())
      .addConnection(userGroupStage.getName(), userSinkStage.getName())
      .addConnection(itemGroupStage.getName(), itemSinkStage.getName())
      .addConnection(maxGroupStage.getName(), maxSinkStage.getName())
      .setEngine(engine)
      .build();
    ApplicationManager appManager = deployETL(config, "groupby-test-" + engine);

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
    runETLOnce(appManager);

    DataSetManager<Table> usersManager = getDataset(usersDatasetName);
    Table usersTable = usersManager.get();
    // users table should have:
    // samuel: 3, 1000000 + 15.34 + 3.14
    Row row = usersTable.get(Bytes.toBytes("samuel"));
    Assert.assertEquals(Objects.requireNonNull(row.getLong("totalPurchases")).longValue(), 3L);
    Assert.assertTrue(
      Math.abs(Objects.requireNonNull(row.getDouble("totalSpent")) - 1000000d - 15.34d - 3.14d) < 0.0000001
    );
    // john: 2, 3.14 + 20.53
    row = usersTable.get(Bytes.toBytes("john"));
    Assert.assertEquals(Objects.requireNonNull(row.getLong("totalPurchases")).longValue(), 2L);
    Assert.assertTrue(Math.abs(Objects.requireNonNull(row.getDouble("totalSpent")) - 3.14d - 20.53d) < 0.0000001);

    DataSetManager<Table> itemsManager = getDataset(itemsDatasetName);
    Table itemsTable = itemsManager.get();
    // items table should have:
    // island: 1, 1234567890000
    row = itemsTable.get(Bytes.toBytes("island"));
    Assert.assertEquals(Objects.requireNonNull(row.getLong("totalPurchases")).longValue(), 1L);
    Assert.assertEquals(Objects.requireNonNull(row.getLong("latestPurchase")).longValue(), 1234567890000L);
    // pie: 2, 1234567890002
    row = itemsTable.get(Bytes.toBytes("pie"));
    Assert.assertEquals(Objects.requireNonNull(row.getLong("totalPurchases")).longValue(), 2L);
    Assert.assertEquals(Objects.requireNonNull(row.getLong("latestPurchase")).longValue(), 1234567890002L);
    // shirt: 2, 1234567890003
    row = itemsTable.get(Bytes.toBytes("shirt"));
    Assert.assertEquals(Objects.requireNonNull(row.getLong("totalPurchases")).longValue(), 2L);
    Assert.assertEquals(Objects.requireNonNull(row.getLong("latestPurchase")).longValue(), 1234567890003L);

    DataSetManager<Table> maxManager = getDataset(maxDatasetName);
    Table maxTable = maxManager.get();
    // max table should have:
    // Samuel, 100000
    row = maxTable.get(Bytes.toBytes("samuel"));
    Assert.assertTrue(Math.abs(row.getDouble("price") - 1000000d) < 0.0000001);
    // pie: 2, 1234567890002
    row = maxTable.get(Bytes.toBytes("john"));
    Assert.assertTrue(Math.abs(row.getDouble("price") - 20.53d) < 0.0000001);
  }

  @Test
  public void testGroupByCollectList() {
      /*
        <ts, user, item, price> --> group by user, itemList:CollectList(item) --> user table
     */
    Schema purchaseSchema = Schema.recordOf(
            "purchase",
            Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
            Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    Schema aggSchema = Schema.recordOf(
            "purchase.agg",
            Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("itemList", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    GroupByConfig groupByConfig = new GroupByConfig("user",  "itemList:CollectList(item)");
    GroupByAggregator groupByAggregator = new GroupByAggregator(groupByConfig);
    MockPipelineConfigurer mockConfigurer = new MockPipelineConfigurer(purchaseSchema, Collections.emptyMap());
    groupByAggregator.configurePipeline(mockConfigurer);
    Assert.assertEquals(aggSchema, mockConfigurer.getOutputSchema());
  }

  @Test
  public void testGroupByCollectSet() {
      /*
        <ts, user, item, price> --> group by item, uniqueUsers:CollectSet(user) --> item table
     */
    Schema purchaseSchema = Schema.recordOf(
            "purchase",
            Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
            Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    Schema aggSchema = Schema.recordOf(
            "purchase.agg",
            Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("uniqueUsers", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    GroupByConfig groupByConfig = new GroupByConfig("item",  "uniqueUsers:CollectSet(user)");
    GroupByAggregator groupByAggregator = new GroupByAggregator(groupByConfig);
    MockPipelineConfigurer mockConfigurer = new MockPipelineConfigurer(purchaseSchema, Collections.emptyMap());
    groupByAggregator.configurePipeline(mockConfigurer);
    Assert.assertEquals(aggSchema, mockConfigurer.getOutputSchema());
  }

  @Test
  public void testGroupByCountDistinct() throws Exception {
    String customerDataset = "customers";
    String uniqueCustomersDataset = "uniqCustomersByState";
      /*
        <customer, state> --> group by state, uniqueUsers:CountDistinct(customer)
     */
    Schema customerSchema = Schema.recordOf(
      "customers",
      Schema.Field.of("customer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("state", Schema.of(Schema.Type.STRING)));
    // expected output schema
    Schema outputSchema = Schema.recordOf(
      "uniqueCustomers",
      Schema.Field.of("state", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("numUniqueCustomers", Schema.of(Schema.Type.INT)));

    // generate pipeline
    ETLStage source = new ETLStage("raw_customers", MockSource.getPlugin(customerDataset, customerSchema));
    ETLStage groupBy =
      new ETLStage("groupBy",
                   new ETLPlugin("GroupByAggregate",
                                 GroupByAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "aggregates", "numUniqueCustomers:CountDistinct(customer)",
                                   "groupByFields", "state"),
                                 null));
    ETLStage sink = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, uniqueCustomersDataset),
                            null));
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(groupBy)
      .addStage(sink)
      .addConnection(source.getName(), groupBy.getName())
      .addConnection(groupBy.getName(), sink.getName())
      .build();

    // deploy pipeline, ingest data and run
    ApplicationManager appManager = deployETL(config, UUID.randomUUID().toString());
    ingestData(customerDataset, customerSchema);
    runETLOnce(appManager);

    // verification
    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(uniqueCustomersDataset);
    TimePartitionedFileSet fileSet = outputManager.get();
    try {
      Set<GenericRecord> actual = Sets.newHashSet(readOutput(fileSet, outputSchema));
      org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(outputSchema.toString());
      Set<GenericRecord> expected = ImmutableSet.of(
        new GenericRecordBuilder(avroOutputSchema)
          .set("state", "California")
          .set("numUniqueCustomers", 4)
          .build(),
        new GenericRecordBuilder(avroOutputSchema)
          .set("state", "Washington")
          .set("numUniqueCustomers", 2)
          .build(),
        new GenericRecordBuilder(avroOutputSchema)
          .set("state", "Texas")
          .set("numUniqueCustomers", 1)
          .build()
      );
      Assert.assertEquals(expected, actual);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testCountDistinctValidation() {
    String groupByField = "state";
    Schema customerSchema = Schema.recordOf(
      "customers",
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("record", Schema.recordOf("record")),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("enum", Schema.enumWith("A", "B")),
      Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(groupByField, Schema.of(Schema.Type.STRING)));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(customerSchema, Collections.emptyMap());
    validateCountDistinct(configurer, "string", groupByField);
    validateCountDistinct(configurer, "int", groupByField);
    validateCountDistinct(configurer, "long", groupByField);
    validateCountDistinct(configurer, "boolean", groupByField);
    assertCountDistinctFailure(configurer, "float", groupByField, "CountDistinct for floats should fail validation");
    assertCountDistinctFailure(configurer, "double", groupByField, "CountDistinct for doubles should fail validation");
    assertCountDistinctFailure(configurer, "array", groupByField, "CountDistinct for arrays should fail validation");
    assertCountDistinctFailure(configurer, "record", groupByField, "CountDistinct for records should fail validation");
    assertCountDistinctFailure(configurer, "map", groupByField, "CountDistinct for maps should fail validation");
    assertCountDistinctFailure(configurer, "enum", groupByField, "CountDistinct for enums should fail validation");
    assertCountDistinctFailure(configurer, "union", groupByField, "CountDistinct for unions should fail validation");
    assertCountDistinctFailure(configurer, "bytes", groupByField, "CountDistinct for bytes should fail validation");
  }

  private void assertCountDistinctFailure(MockPipelineConfigurer configurer, String aggregateField,
                                          String groupByField, String failureReason) {
    try {
      validateCountDistinct(configurer, aggregateField, groupByField);
      Assert.fail(failureReason);
    } catch (ValidationException e) {
      // expected
    }
  }

  private void validateCountDistinct(MockPipelineConfigurer configurer, String aggregateField, String groupByField) {
    String aggregates = String.format("distinct:CountDistinct(%s)", aggregateField);
    GroupByConfig groupByConfig = new GroupByConfig(groupByField,  aggregates);
    GroupByAggregator groupByAggregator = new GroupByAggregator(groupByConfig);
    groupByAggregator.configurePipeline(configurer);
  }

  private void ingestData(String datasetName, Schema schema) throws Exception {
    // write input data
    DataSetManager<Table> datasetManager = getDataset(datasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("customer", "Rodrigues")
        .set("state", "California")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Johnson")
        .set("state", "Washington")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Smith")
        .set("state", "Texas")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Sawyer")
        .set("state", "Washington")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Sparrow")
        .set("state", "California")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Smith")
        .set("state", "California")
        .build(),
      StructuredRecord.builder(schema)
        .set("customer", "Murphy")
        .set("state", "California")
        .build());
    MockSource.writeInput(datasetManager, input);
  }
}
