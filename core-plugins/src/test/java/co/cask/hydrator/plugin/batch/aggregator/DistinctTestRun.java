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
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for GroupBy Aggregator.
 */
public class DistinctTestRun extends ETLBatchTestBase {

  @Test
  public void testDistinct() throws Exception {
    String inputDatasetName = "distinct-input";
    String outputDatasetName = "distinct-output";

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    Schema outputSchema = Schema.recordOf(
      "purchase.distinct",
      Schema.Field.of("user_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)));

    ETLStage sourceStage = new ETLStage(
      "purchases", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, inputDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA, inputSchema.toString()),
                                 null));

    ETLStage distinctStage = new ETLStage(
      "distinct", new ETLPlugin("Distinct", BatchAggregator.PLUGIN_TYPE,
                                ImmutableMap.of("fields", "user_name,item"), null));


    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputDatasetName),
                            null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(distinctStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), distinctStage.getName())
      .addConnection(distinctStage.getName(), sinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, "distinct-test");

    // write input data
    DataSetManager<Table> purchaseManager = getDataset(inputDatasetName);
    Table purchaseTable = purchaseManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("ts", 1234567890000L);
    put.add("user_name", "samuel");
    put.add("item", "shirt");
    put.add("price", 10d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("ts", 1234567890001L);
    put.add("user_name", "samuel");
    put.add("item", "shirt");
    put.add("price", 15.34d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("ts", 1234567890001L);
    put.add("user_name", "samuel");
    put.add("item", "pie");
    put.add("price", 3.14d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("ts", 1234567890002L);
    put.add("user_name", "samuel");
    put.add("item", "pie");
    put.add("price", 3.14d);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(5));
    put.add("ts", 1234567890003L);
    put.add("user_name", "samuel");
    put.add("item", "shirt");
    put.add("price", 20.53d);
    purchaseTable.put(put);
    purchaseManager.flush();

    // run the pipeline
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> records = readOutput(fileSet, outputSchema);
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
}
