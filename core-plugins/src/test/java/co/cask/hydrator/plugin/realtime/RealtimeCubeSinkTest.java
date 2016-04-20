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

package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RealtimeCubeSinkTest extends ETLRealtimeTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void test() throws Exception {
    ETLPlugin source = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                     ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                                     Constants.Reference.REFERENCE_NAME, "DG"),
                                     null);
    // single aggregation
    String aggregationGroup = "byName:name";
    String measurement = "score:GAUGE";

    ETLPlugin sink = new ETLPlugin("Cube", RealtimeSink.PLUGIN_TYPE,
                                   ImmutableMap.of(Properties.Cube.DATASET_NAME, "cube1",
                                                   Properties.Cube.AGGREGATIONS, aggregationGroup,
                                                   Properties.Cube.MEASUREMENTS, measurement),
                                   null);
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("sink", sink))
      .addConnection("source", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testCubeSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    final long startTs = System.currentTimeMillis() / 1000;

    workerManager.start();
    final DataSetManager<Cube> tableManager = getDataset("cube1");
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Cube cube = tableManager.get();
        Collection<TimeSeries> result = cube.query(buildCubeQuery(startTs));
        return !result.isEmpty();
      }
    }, 10, TimeUnit.SECONDS);
    workerManager.stop();

    // verify
    Cube cube = tableManager.get();
    Collection<TimeSeries> result = cube.query(buildCubeQuery(startTs));

    Iterator<TimeSeries> iterator = result.iterator();
    Assert.assertTrue(iterator.hasNext());
    TimeSeries timeSeries = iterator.next();
    Assert.assertEquals("score", timeSeries.getMeasureName());
    Assert.assertFalse(timeSeries.getTimeValues().isEmpty());
    Assert.assertEquals(3, timeSeries.getTimeValues().get(0).getValue());
    Assert.assertFalse(iterator.hasNext());
  }

  private CubeQuery buildCubeQuery(long startTs) {
    long endTs = System.currentTimeMillis() / 1000;
    return CubeQuery.builder()
      .select().measurement("score", AggregationFunction.LATEST)
      .from("byName").resolution(1, TimeUnit.SECONDS)
      .where().timeRange(startTs, endTs).limit(100).build();
  }
}
