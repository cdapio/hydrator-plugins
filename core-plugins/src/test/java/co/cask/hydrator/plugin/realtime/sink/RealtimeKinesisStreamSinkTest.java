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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.realtime.ETLRealtimeTestBase;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link RealtimeKinesisStreamSink}
 */
public class RealtimeKinesisStreamSinkTest extends ETLRealtimeTestBase {

  @Test(expected = RuntimeException.class)
  public void kinesisSinkWithinvalidKeys() throws Exception {

    ETLPlugin source = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                     ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.STREAM_TYPE,
                                                     Constants.Reference.REFERENCE_NAME, "DG"),
                                     null);

    Map<String, String> properties = new HashMap<>();
    properties.put(Properties.KinesisRealtimeSink.NAME, "unitTest");
    properties.put(Properties.KinesisRealtimeSink.ACCESS_ID, "accessId");
    properties.put(Properties.KinesisRealtimeSink.ACCESS_KEY, "accessKeySecret");
    properties.put(Properties.KinesisRealtimeSink.BODY_FIELD, "body");
    properties.put(Properties.KinesisRealtimeSink.SHARD_COUNT, "1");
    properties.put(Properties.KinesisRealtimeSink.PARTITION_KEY, "part");

    ETLPlugin sink = new ETLPlugin("KinesisSink", RealtimeSink.PLUGIN_TYPE, properties, null);

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("sink", sink))
      .addConnection("source", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testCubeSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    TimeUnit.SECONDS.sleep(60);
    workerManager.stop();
  }
}
