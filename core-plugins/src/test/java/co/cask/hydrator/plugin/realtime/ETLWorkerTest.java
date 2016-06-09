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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupTableConfig;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLRealtimeApplication}.
 */
public class ETLWorkerTest extends ETLRealtimeTestBase {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, true);

  @Test
  public void testEmptyProperties() throws Exception {
    // Set properties to null to test if ETLTemplate can handle it.
    ETLPlugin sourceConfig = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                           ImmutableMap.of(co.cask.hydrator.common.Constants.Reference.REFERENCE_NAME,
                                                           "DG"), null);
    ETLPlugin sinkConfig = new ETLPlugin("Stream", RealtimeSink.PLUGIN_TYPE,
                                         ImmutableMap.of(Properties.Stream.NAME, "testS"), null);
    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .setInstances(2)
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testAdap");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    Assert.assertNotNull(appManager);
    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForStatus(true, 10, 1);
    Assert.assertEquals(2, workerManager.getInstances());
    workerManager.stop();
    workerManager.waitForStatus(false, 10, 1);
  }

  @Test
  public void testStreamSinks() throws Exception {
    ETLPlugin sourceConfig = new ETLPlugin(
      "DataGenerator", RealtimeSource.PLUGIN_TYPE,
      ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.STREAM_TYPE,
                      co.cask.hydrator.common.Constants.Reference.REFERENCE_NAME, "DG"), null);

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(new ETLStage("sink1", new ETLPlugin("Stream", RealtimeSink.PLUGIN_TYPE,
                                                    ImmutableMap.of(Properties.Stream.NAME, "streamA"), null)))
      .addStage(new ETLStage("sink2", new ETLPlugin("Stream", RealtimeSink.PLUGIN_TYPE,
                                                    ImmutableMap.of(Properties.Stream.NAME, "streamB"), null)))
      .addStage(new ETLStage("sink3", new ETLPlugin("Stream", RealtimeSink.PLUGIN_TYPE,
                                                    ImmutableMap.of(Properties.Stream.NAME, "streamC"), null)))
      .addConnection(source.getName(), "sink1")
      .addConnection(source.getName(), "sink2")
      .addConnection(source.getName(), "sink3")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    long startTime = System.currentTimeMillis();
    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();

    List<StreamManager> streamManagers = Lists.newArrayList(
      getStreamManager(Id.Namespace.DEFAULT, "streamA"),
      getStreamManager(Id.Namespace.DEFAULT, "streamB"),
      getStreamManager(Id.Namespace.DEFAULT, "streamC")
    );

    int retries = 0;
    boolean succeeded = false;
    while (retries < 10) {
      succeeded = checkStreams(streamManagers, startTime);
      if (succeeded) {
        break;
      }
      retries++;
      TimeUnit.SECONDS.sleep(1);
    }

    workerManager.stop();
    Assert.assertTrue(succeeded);
  }

  @Test
  public void testScriptLookup() throws Exception {
    addDatasetInstance(KeyValueTable.class.getName(), "lookupTable");
    DataSetManager<KeyValueTable> lookupTable = getDataset("lookupTable");
    lookupTable.get().write("Bob".getBytes(Charsets.UTF_8), "123".getBytes(Charsets.UTF_8));
    lookupTable.flush();

    Schema.Field idField = Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT)));
    Schema.Field nameField = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
    Schema.Field scoreField = Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE));
    Schema.Field graduatedField = Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN));
    Schema.Field binaryNameField = Schema.Field.of("binary", Schema.of(Schema.Type.BYTES));
    Schema.Field timeField = Schema.Field.of("time", Schema.of(Schema.Type.LONG));
    Schema schema =  Schema.recordOf("tableRecord", idField, nameField, scoreField, graduatedField,
                                     binaryNameField, timeField);

    ETLPlugin source = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                     ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                                     co.cask.hydrator.common.Constants.Reference.REFERENCE_NAME, "DG"),
                                     null);
    ETLPlugin transform = new ETLPlugin(
      "Script",
      Transform.PLUGIN_TYPE,
      ImmutableMap.of(
        "script", "function transform(x, ctx) { " +
          "x.name = x.name + '..hi..' + ctx.getLookup('lookupTable').lookup(x.name); return x; }",
        "lookup", GSON.toJson(new LookupConfig(ImmutableMap.of(
          "lookupTable", new LookupTableConfig(LookupTableConfig.TableType.DATASET)
        )))),
      null);
    ETLPlugin sink =
      new ETLPlugin("Table", RealtimeSink.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.Table.NAME, "testScriptLookup_table1",
                                    Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary",
                                    Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                    null);

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("sink", sink))
      .addStage(new ETLStage("transform", transform))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    workerManager.start();
    DataSetManager<Table> tableManager = getDataset("testScriptLookup_table1");
    waitForTableToBePopulated(tableManager);
    workerManager.stop();

    // verify
    Table table = tableManager.get();
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals("Bob..hi..123", row.getString("name"));

    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("select name from dataset_testScriptLookup_table1").executeQuery();
    Assert.assertTrue(results.next());
    Assert.assertEquals("Bob..hi..123", results.getString(1));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTableSink() throws Exception {
    Schema.Field idField = Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT)));
    Schema.Field nameField = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
    Schema.Field scoreField = Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE));
    Schema.Field graduatedField = Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN));
    // nullable row key field to test cdap-3239
    Schema.Field binaryNameField = Schema.Field.of("binary", Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    Schema.Field timeField = Schema.Field.of("time", Schema.of(Schema.Type.LONG));
    Schema schema =  Schema.recordOf("tableRecord", idField, nameField, scoreField, graduatedField,
                                     binaryNameField, timeField);

    ETLPlugin source = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                     ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                                     co.cask.hydrator.common.Constants.Reference.REFERENCE_NAME, "DG"),
                                     null);
    ETLPlugin sink = new ETLPlugin("Table", RealtimeSink.PLUGIN_TYPE,
                                   ImmutableMap.of(Properties.Table.NAME, "table1",
                                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary",
                                                   Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                                   null);
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("sink", sink))
      .addConnection("source", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    workerManager.start();
    DataSetManager<Table> tableManager = getDataset("table1");
    waitForTableToBePopulated(tableManager);
    workerManager.stop();

    // verify
    Table table = tableManager.get();
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals(1, (int) row.getInt("id"));
    Assert.assertEquals("Bob", row.getString("name"));
    Assert.assertEquals(3.4, row.getDouble("score"), 0.000001);
    // binary field was the row key and thus shouldn't be present in the columns
    Assert.assertNull(row.get("binary"));
    Assert.assertNotNull(row.getLong("time"));

    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("select `binary`,name,score from dataset_table1").executeQuery();
    Assert.assertTrue(results.next());
    Assert.assertArrayEquals("Bob".getBytes(Charsets.UTF_8), results.getBytes(1));
    Assert.assertEquals("Bob", results.getString(2));
    Assert.assertEquals(3.4, results.getDouble(3), 0.000001);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDAG() throws Exception {

    Schema.Field idField = Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT)));
    Schema.Field nameField = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
    Schema.Field scoreField = Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE));
    Schema.Field graduatedField = Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN));
    // nullable row key field to test cdap-3239
    Schema.Field binaryNameField = Schema.Field.of("binary", Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    Schema.Field timeField = Schema.Field.of("time", Schema.of(Schema.Type.LONG));
    Schema schema =  Schema.recordOf("tableRecord", idField, nameField, scoreField, graduatedField,
                                     binaryNameField, timeField);

    ETLPlugin source = new ETLPlugin("DataGenerator", RealtimeSource.PLUGIN_TYPE,
                                     ImmutableMap.of(
                                       DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                       co.cask.hydrator.common.Constants.Reference.REFERENCE_NAME, "DG"),
                                     null);

    ETLPlugin sinkConfig1 = new ETLPlugin("Table", RealtimeSink.PLUGIN_TYPE,
                                          ImmutableMap.of(Properties.Table.NAME, "table1",
                                                          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary",
                                                          Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                                          null);

    ETLPlugin sinkConfig2 = new ETLPlugin("Table", RealtimeSink.PLUGIN_TYPE,
                                          ImmutableMap.of(Properties.Table.NAME, "table2",
                                                          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary",
                                                          Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                                          null);

    String script = "function transform(x, context) {  " +
      "x.name = \"Rob\";" +
      "x.id = 2;" +
      "return x;" +
      "};";
    ETLPlugin transformConfig = new ETLPlugin("Script", Transform.PLUGIN_TYPE, ImmutableMap.of("script", script), null);

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("sink1", sinkConfig1))
      .addStage(new ETLStage("sink2", sinkConfig2))
      .addStage(new ETLStage("transform", transformConfig))
      .addConnection("source", "sink1")
      .addConnection("source", "transform")
      .addConnection("transform", "sink2")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    workerManager.start();
    DataSetManager<Table> tableManager1 = getDataset("table1");
    waitForTableToBePopulated(tableManager1);
    DataSetManager<Table> tableManager2 = getDataset("table2");
    waitForTableToBePopulated(tableManager2);
    workerManager.stop();

    // verify
    Table table = tableManager1.get();
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals("Bob", row.getString("name"));
    Assert.assertEquals(1, (int) row.getInt("id"));
    Assert.assertEquals(3.4, row.getDouble("score"), 0.000001);
    // binary field was the row key and thus shouldn't be present in the columns
    Assert.assertNull(row.get("binary"));
    Assert.assertNotNull(row.getLong("time"));

    // verify that table2 doesn't have these records
    tableManager2 = getDataset("table2");
    table = tableManager2.get();

    row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals(2, (int) row.getInt("id"));
    // transformed
    Assert.assertEquals("Rob", row.getString("name"));
    Assert.assertEquals(3.4, row.getDouble("score"), 0.000001);
    // binary field was the row key and thus shouldn't be present in the columns
    Assert.assertNull(row.get("binary"));
    Assert.assertNotNull(row.getLong("time"));
  }


  private void waitForTableToBePopulated(final DataSetManager<Table> tableManager) throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Table table = tableManager.get();
        Row row = table.get("Bob".getBytes(Charsets.UTF_8));
        // need to wait for information to get to the table, not just for the row to be created
        return row.getColumns().size() != 0;
      }
    }, 10, TimeUnit.SECONDS);
  }

  private boolean checkStreams(Collection<StreamManager> streamManagers, long startTime) throws IOException {
    try {
      long currentDiff = System.currentTimeMillis() - startTime;
      for (StreamManager streamManager : streamManagers) {
        List<StreamEvent> streamEvents = streamManager.getEvents("now-" + Long.toString(currentDiff) + "ms", "now",
                                                                 Integer.MAX_VALUE);
        // verify that some events were sent to the stream
        Assert.assertTrue(streamEvents.size() > 0);
        // since we sent all identical events, verify the contents of just one of them
        Random random = new Random();
        StreamEvent event = streamEvents.get(random.nextInt(streamEvents.size()));
        ByteBuffer body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        if (headers != null && !headers.isEmpty()) {
          // check h1 header has value v1
          if (!"v1".equals(headers.get("h1"))) {
            return false;
          }
        }
        // check body has content "Hello"
        if (!"Hello".equals(Bytes.toString(body, Charsets.UTF_8))) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      // streamManager.getEvents() can throw an exception if there is nothing in the stream
      return false;
    }
  }
}
