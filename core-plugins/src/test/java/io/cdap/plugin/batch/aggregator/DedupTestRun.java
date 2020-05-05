/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test for Dedup Aggregator.
 */
public class DedupTestRun extends ETLBatchTestBase {
  private static final Schema PURCHASE_SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("fname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("lname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ts", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
  private static final Schema SINK_SCHEMA = Schema.recordOf(PURCHASE_SCHEMA.getRecordName() + ".dedup",
                                                            PURCHASE_SCHEMA.getFields());

  private void testHelper(String appName, String purchasesDatasetName, ETLStage purchaseStage,
                          ETLStage dedupStage, String sinkDatasetName, ETLStage sinkStage,
                          Map<String, String> runTimeProperties) throws Exception {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(purchaseStage)
      .addStage(dedupStage)
      .addStage(sinkStage)
      .addConnection(purchaseStage.getName(), dedupStage.getName())
      .addConnection(dedupStage.getName(), sinkStage.getName())
      .build();

    ApplicationManager appManager = deployETL(config, appName);

    // write input data
    // 1: samuel, goel, 10, 100.31
    // 2: samuel, goel, 11, 200.43
    // 3: john, desai, 5, 300.45
    // 4: john, desai, 1, 400.12

    DataSetManager<Table> purchaseManager = getDataset(purchasesDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(PURCHASE_SCHEMA)
        .set("fname", "samuel")
        .set("lname", "goel")
        .set("ts", 10)
        .set("price", 100.31)
        .build(),
      StructuredRecord.builder(PURCHASE_SCHEMA)
        .set("fname", "samuel")
        .set("lname", "goel")
        .set("ts", 11)
        .set("price", 200.43)
        .build(),
      StructuredRecord.builder(PURCHASE_SCHEMA)
        .set("fname", "john")
        .set("lname", "desai")
        .set("ts", 5)
        .set("price", 300.45)
        .build(),
      StructuredRecord.builder(PURCHASE_SCHEMA)
        .set("fname", "john")
        .set("lname", "desai")
        .set("ts", 1)
        .set("price", 400.12)
        .build());
    MockSource.writeInput(purchaseManager, input);

    runETLOnce(appManager, runTimeProperties);

    DataSetManager<Table> sinkManager = getDataset(sinkDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(sinkManager);
    Assert.assertEquals("Expected records", 2, output.size());
    List<StructuredRecord> expectedOutput = ImmutableList.of(
      StructuredRecord.builder(SINK_SCHEMA)
        .set("fname", "samuel")
        .set("lname", "goel")
        .set("ts", 11)
        .set("price", 200.43)
        .build(),
      StructuredRecord.builder(SINK_SCHEMA)
        .set("fname", "john")
        .set("lname", "desai")
        .set("ts", 5)
        .set("price", 300.45)
        .build());
    Assert.assertEquals(Sets.newHashSet(output), Sets.newHashSet(expectedOutput));
  }

  @Test
  public void testDedup() throws Exception {
    String purchasesDatasetName = "purchases";
    String sinkDatasetName = "sinkDataset";
    String appName = "dedup-test";
    ETLStage purchaseStage = new ETLStage("purchases", MockSource.getPlugin(purchasesDatasetName, PURCHASE_SCHEMA));
    ETLStage dedupStage = new ETLStage("dedupStage", new ETLPlugin(
      "Deduplicate", BatchAggregator.PLUGIN_TYPE, ImmutableMap.of("uniqueFields", "fname,lname",
      "filterOperation", "ts:max"), null));
    ETLStage sinkStage = new ETLStage("tableSink", MockSink.getPlugin(sinkDatasetName));

    testHelper(appName, purchasesDatasetName, purchaseStage, dedupStage,
      sinkDatasetName, sinkStage, ImmutableMap.of());
  }

  @Test
  public void testDedupWithMacro() throws Exception {
    String purchasesDatasetName = "purchases-null-inputschema";
    String sinkDatasetName = "sinkDataset-null-inputschema";
    String appName = "dedup-test-null-inputschema";
    ETLStage purchaseStage = new ETLStage("purchases", MockSource.getPlugin(purchasesDatasetName));
    ETLStage dedupStage = new ETLStage("dedupStage", new ETLPlugin(
      "Deduplicate", BatchAggregator.PLUGIN_TYPE, ImmutableMap.of("uniqueFields", "${uniqueFields}",
      "filterOperation", "${filterOperation}", "numPartitions", "${numPartitions}"), null));
    ETLStage sinkStage = new ETLStage("tableSink", MockSink.getPlugin(sinkDatasetName));

    testHelper(appName, purchasesDatasetName, purchaseStage, dedupStage,
      sinkDatasetName, sinkStage,
      ImmutableMap.of("uniqueFields", "fname,lname", "filterOperation", "ts:max", "numPartitions", "2"));
  }
}
