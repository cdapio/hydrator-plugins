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

import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for Dedup Aggregator.
 */
public class DedupTestRun extends ETLBatchTestBase {

  @Test
  public void testDedup() throws Exception {
    String purchasesDatasetName = "purchases";
    String sinkDatasetName = "sinkDataset";

    Schema purchaseSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("fname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("lname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ts", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

    ETLStage purchaseStage = new ETLStage("purchases", new ETLPlugin(
      "Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(Properties.BatchReadableWritable.NAME, purchasesDatasetName,
                                                        Properties.Table.PROPERTY_SCHEMA, purchaseSchema.toString()),
      null));
    ETLStage dedupStage = new ETLStage("dedupStage", new ETLPlugin(
      "Deduplicate", BatchAggregator.PLUGIN_TYPE, ImmutableMap.of("uniqueFields", "fname,lname",
                                                                  "filterOperation", "ts:max"), null));

    Schema sinkSchema = Schema.recordOf("sinkSchema", Schema.Field.of("fname", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("lname", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("ts", Schema.of(Schema.Type.INT)),
                                        Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    ETLStage sinkStage = new ETLStage("tableSink", new ETLPlugin(
      "Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(Properties.BatchReadableWritable.NAME, sinkDatasetName,
                                                      Properties.Table.PROPERTY_SCHEMA, sinkSchema.toString(),
                                                      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ts"), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(purchaseStage)
      .addStage(dedupStage)
      .addStage(sinkStage)
      .addConnection(purchaseStage.getName(), dedupStage.getName())
      .addConnection(dedupStage.getName(), sinkStage.getName())
      .build();

    ApplicationManager appManager = deployETL(config, "dedup-test");

    // write input data
    // 1: samuel, goel, 10, 100.31
    // 2: samuel, goel, 11, 200.43
    // 3: john, desai, 5, 300.45
    // 4: john, desai, 1, 400.12

    DataSetManager<Table> purchaseManager = getDataset(purchasesDatasetName);
    Table purchaseTable = purchaseManager.get();

    Put put = new Put(Bytes.toBytes(1));
    put.add("fname", "samuel");
    put.add("lname", "goel");
    put.add("ts", 10);
    put.add("price", 100.31);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("fname", "samuel");
    put.add("lname", "goel");
    put.add("ts", 11);
    put.add("price", 200.43);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("fname", "john");
    put.add("lname", "desai");
    put.add("ts", 5);
    put.add("price", 300.45);
    purchaseTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("fname", "john");
    put.add("lname", "desai");
    put.add("ts", 1);
    put.add("price", 400.12);
    purchaseTable.put(put);
    purchaseManager.flush();

    runETLOnce(appManager);

    DataSetManager<Table> sinkManager = getDataset(sinkDatasetName);
    try (Table sinkTable = sinkManager.get()) {

      // table should have:
      // 11 : samuel, goel, 200.43
      // 5 : john, desai, 300.45
      Row row = sinkTable.get(Bytes.toBytes(11));
      Assert.assertEquals("samuel", row.getString("fname"));
      Assert.assertEquals("goel", row.getString("lname"));
      Assert.assertEquals(200.43, row.getDouble("price"), 0.0001);

      row = sinkTable.get(Bytes.toBytes(5));
      Assert.assertEquals("john", row.getString("fname"));
      Assert.assertEquals("desai", row.getString("lname"));
      Assert.assertEquals(300.45, row.getDouble("price"), 0.0001);
    }
  }
}
