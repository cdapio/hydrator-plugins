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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for Kinesis batch sink
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

    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    // Throws illegal argument exception because of invalid AWS keys
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    Assert.assertEquals(mrManager.getHistory().get(0).getStatus(), ProgramRunStatus.FAILED);
  }
}
