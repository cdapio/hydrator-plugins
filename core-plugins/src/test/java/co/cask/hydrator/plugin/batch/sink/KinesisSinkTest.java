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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for Kinesis configuration
 */
public class KinesisSinkTest extends ETLBatchTestBase {



  @Test
  public void test() throws Exception {

    Schema schema = Schema.recordOf(
      "action",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    Plugin sourceConfig = new Plugin("Table",
                                     ImmutableMap.of(
                                       Properties.BatchReadableWritable.NAME, "KinesisSinkInputTable",
                                       Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                       Properties.Table.PROPERTY_SCHEMA, schema.toString()));
    ETLStage source = new ETLStage("tableSource", sourceConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", "KinesisSinkTest");
    properties.put(Properties.KinesisRealtimeSink.NAME, "unitTest");
    properties.put(Properties.KinesisRealtimeSink.ACCESS_ID, "someId");
    properties.put(Properties.KinesisRealtimeSink.ACCESS_KEY, "SomeSecret");
    properties.put(Properties.KinesisRealtimeSink.BODY_FIELD, "body");
    properties.put(Properties.KinesisRealtimeSink.SHARD_COUNT, "1");
    properties.put(Properties.KinesisRealtimeSink.DISTRIBUTE, "true");

    Plugin sinkConfig = new Plugin("KinesisSink", properties);
    ETLStage sink = new ETLStage("KinesisSinkTest", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testKinesis");
    // This will throw illegal argument exception because of invalid keys
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("KinesisSinkInputTable");
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("body", "samuel");
    put.add("count", 5);

    // Row 2
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.add("body", "name2");
    put2.add("count", 2);

    // Row 3
    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.add("body", "name3");
    put3.add("count", 3);

    inputTable.put(put);
    inputTable.put(put2);
    inputTable.put(put3);
    inputManager.flush();

    long startTs = System.currentTimeMillis() / 1000;

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    long endTs = System.currentTimeMillis() / 1000;
  }
}
