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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.common.Properties;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for Distinct Aggregator.
 */
public class DistinctTestRun extends ETLBatchTestBase {
  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("user_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
  private static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "purchase.distinct",
    Schema.Field.of("user_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("item", Schema.of(Schema.Type.STRING)));

  private void testHelper(Map<String, String> aggProproperties, Map<String, String> runTimeProperties,
                          String namePrefix, Engine engine) throws Exception {
    String inputDatasetName = "distinct-input-" + namePrefix + "-" + engine;
    String outputDatasetName = "distinct-output-" + namePrefix + "-" + engine;
    String appName = "distinct-test" + namePrefix + "-" + engine;

    ETLStage sourceStage = new ETLStage("purchases", MockSource.getPlugin(inputDatasetName, INPUT_SCHEMA));

    ETLStage distinctStage = new ETLStage(
      "distinct", new ETLPlugin("Distinct", BatchReducibleAggregator.PLUGIN_TYPE, aggProproperties, null));

    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, OUTPUT_SCHEMA.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME,
                                            outputDatasetName), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(sourceStage)
      .addStage(distinctStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), distinctStage.getName())
      .addConnection(distinctStage.getName(), sinkStage.getName())
      .setEngine(engine)
      .build();
    ApplicationManager appManager = deployETL(config, appName);

    // write input data
    DataSetManager<Table> purchaseManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("ts", 1234567890000L)
        .set("user_name", "samuel")
        .set("item", "shirt")
        .set("price", 10d)
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("ts", 1234567890001L)
        .set("user_name", "samuel")
        .set("item", "shirt")
        .set("price", 15.34d)
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("ts", 1234567890001L)
        .set("user_name", "samuel")
        .set("item", "pie")
        .set("price", 3.14d)
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("ts", 1234567890002L)
        .set("user_name", "samuel")
        .set("item", "pie")
        .set("price", 3.14d)
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("ts", 1234567890003L)
        .set("user_name", "samuel")
        .set("item", "shirt")
        .set("price", 20.53d)
        .build());
    MockSource.writeInput(purchaseManager, input);

    // run the pipeline
    runETLOnce(appManager, runTimeProperties);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> records = readOutput(fileSet, OUTPUT_SCHEMA);
    Assert.assertEquals(2, records.size());
    Set<String> items = new HashSet<>();
    Set<String> users = new HashSet<>();
    for (GenericRecord record : records) {
      items.add(record.get("item").toString());
      users.add(record.get("user_name").toString());
    }
    Assert.assertEquals(ImmutableSet.of("samuel"), users);
    Assert.assertEquals(ImmutableSet.of("shirt", "pie"), items);
  }

  @Test
  public void testDistinct() throws Exception {
    testHelper(ImmutableMap.of("fields", "user_name,item"), ImmutableMap.of(), "", Engine.SPARK);
    testHelper(ImmutableMap.of("fields", "user_name,item"), ImmutableMap.of(), "", Engine.MAPREDUCE);
  }

  @Test
  public void testDistinctWithMacro() throws Exception {
    testHelper(ImmutableMap.of("fields", "${fields}", "numPartitions", "${numPartitions}"),
               ImmutableMap.of("fields", "user_name,item", "numPartitions", "2"),
               "-unknown-inputschema", Engine.SPARK);
    testHelper(ImmutableMap.of("fields", "${fields}", "numPartitions", "${numPartitions}"),
               ImmutableMap.of("fields", "user_name,item", "numPartitions", "2"),
               "-unknown-inputschema", Engine.MAPREDUCE);
  }
}
