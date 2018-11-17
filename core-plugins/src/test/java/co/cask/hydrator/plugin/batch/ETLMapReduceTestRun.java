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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.batch.source.FileBatchSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTestRun extends ETLBatchTestBase {

  @Test
  public void testInvalidTransformConfigFailsToDeploy() {
    ETLPlugin sourceConfig =
      new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"), null);
    ETLPlugin sink =
      new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"), null);

    ETLPlugin transform = new ETLPlugin("Script", Transform.PLUGIN_TYPE,
                                        ImmutableMap.of("script", "return x;"), null);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", sourceConfig))
      .addStage(new ETLStage("sink", sink))
      .addStage(new ETLStage("transform", transform))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KVToKV");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Ignore
  @Test
  public void testKVToKV() throws Exception {
    // kv table to kv table pipeline
    ETLStage source = new ETLStage(
      "source", new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, "kvTable1"), null));
    ETLStage sink = new ETLStage(
      "sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.BatchReadableWritable.NAME, "kvTable2"), null));
    ETLStage transform = new ETLStage("transform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                                 ImmutableMap.of(), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "KVToKV");

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("kvTable1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 10000; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    runETLOnce(appManager);

    DataSetManager<KeyValueTable> table2 = getDataset("kvTable2");
    try (KeyValueTable outputTable = table2.get()) {
      for (int i = 0; i < 10000; i++) {
        Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
      }
    }
  }

  @Ignore
  @Test
  public void testDAG() throws Exception {

    Schema schema = Schema.recordOf(
      "userNames",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING))
    );
    ETLStage source = new ETLStage(
      "source", new ETLPlugin("Table",
                              BatchSource.PLUGIN_TYPE,
                              ImmutableMap.of(
                                Properties.BatchReadableWritable.NAME, "dagInputTable",
                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                              null));
    ETLStage sink1 = new ETLStage(
      "sink1", new ETLPlugin("Table",
                             BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "dagOutputTable1",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                               Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                             null));
    ETLStage sink2 = new ETLStage(
      "sink2", new ETLPlugin("Table",
                             BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "dagOutputTable2",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                               Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                             null));

    String validationScript = "function isValid(input, context) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.userid, 4)) " +
      "{ errCode = 10; errMsg = 'user name greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    ETLStage transform = new ETLStage(
      "transform", new ETLPlugin("Validator", Transform.PLUGIN_TYPE,
                                 ImmutableMap.of("validators", "core",
                                                 "validationScript", validationScript),
                                 null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), transform.getName())
      .addConnection(source.getName(), sink1.getName())
      .addConnection(transform.getName(), sink2.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "DagApp");

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("dagInputTable");
    Table inputTable = inputManager.get();

    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      // valid record, user name "sam[0-9]" is 4 chars long for validator transform
      put.add("userid", "sam" + i);
      inputTable.put(put);
      inputManager.flush();

      Put put2 = new Put(Bytes.toBytes("row" + (i + 10)));
      // invalid record, user name "sam[10-19]" is 5 chars long and invalid according to validator transform
      put2.add("userid", "sam" + (i + 10));
      inputTable.put(put2);
      inputManager.flush();
    }

    runETLOnce(appManager);

    // all records are passed to this table (validation not performed)
    DataSetManager<Table> outputManager1 = getDataset("dagOutputTable1");
    Table outputTable1 = outputManager1.get();
    for (int i = 0; i < 20; i++) {
      Row row = outputTable1.get(Bytes.toBytes("row" + i));
      Assert.assertEquals("sam" + i, row.getString("userid"));
    }

    // only 10 records are passed to this table (validation performed)
    DataSetManager<Table> outputManager2 = getDataset("dagOutputTable2");
    Table outputTable2 = outputManager2.get();
    for (int i = 0; i < 10; i++) {
      Row row = outputTable2.get(Bytes.toBytes("row" + i));
      Assert.assertEquals("sam" + i, row.getString("userid"));
    }
    for (int i = 10; i < 20; i++) {
      Row row = outputTable2.get(Bytes.toBytes("row" + i));
      Assert.assertNull(row.getString("userid"));
    }
  }

  @Ignore
  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTableToTableWithValidations() throws Exception {

    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );

    ETLStage source = new ETLStage(
      "source", new ETLPlugin("Table",
                              BatchSource.PLUGIN_TYPE,
                              ImmutableMap.of(
                                Properties.BatchReadableWritable.NAME, "inputTable",
                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                              null));

    String validationScript = "function isValid(input) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.user, 6)) " +
      "{ errCode = 10; errMsg = 'user name greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    ETLStage transform = new ETLStage(
      "transform",
      new ETLPlugin("Validator",
                    Transform.PLUGIN_TYPE,
                    ImmutableMap.of("validators", "core",
                                    "validationScript", validationScript)));

    ETLStage sink1 = new ETLStage(
      "sink1", new ETLPlugin("Table",
                             BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "outputTable",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                               Properties.Table.PROPERTY_SCHEMA, schema.toString())));

    ETLStage sink2 = new ETLStage(
      "sink2", new ETLPlugin("Table",
                             BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "keyErrors",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                               Properties.Table.PROPERTY_SCHEMA, schema.toString())));

    ETLStage errorCollector = new ETLStage(
      "errors", new ETLPlugin("ErrorCollector",
                              ErrorTransform.PLUGIN_TYPE,
                              ImmutableMap.of()));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(errorCollector)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink1.getName())
      .addConnection(transform.getName(), errorCollector.getName())
      .addConnection(errorCollector.getName(), sink2.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "TableToTable");

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("inputTable");
    Table inputTable = inputManager.get();

    // valid record, user name "samuel" is 6 chars long
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();

    // valid record, user name "jackson" is > 6 characters
    put = new Put(Bytes.toBytes("row2"));
    put.add("user", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);
    inputManager.flush();

    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("user"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertTrue(row.isEmpty());

    DataSetManager<Table> errorManager = getDataset("keyErrors");
    try (Table errorTable = errorManager.get()) {
      row = errorTable.get(Bytes.toBytes("row1"));
      Assert.assertTrue(row.isEmpty());
      row = errorTable.get(Bytes.toBytes("row2"));
      Assert.assertEquals("jackson", row.getString("user"));
      Assert.assertEquals(10, (int) row.getInt("count"));
      Assert.assertTrue(Math.abs(123456789d - row.getDouble("price")) < 0.000001);
      Assert.assertEquals("island", row.getString("item"));
    }
  }

  @Ignore
  @Test
  public void testFiletoMultipleTPFS() throws Exception {
    String filePath = "file:///tmp/test/text.txt";
    String testData = "String for testing purposes.";

    Path textFile = new Path(filePath);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream writeData = fs.create(textFile);
    writeData.write(testData.getBytes());
    writeData.flush();
    writeData.close();

    ETLStage source = new ETLStage(
      "source", new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
                              ImmutableMap.<String, String>builder()
                                .put(Constants.Reference.REFERENCE_NAME, "TestFile")
                                .put(Properties.File.FILESYSTEM, "Text")
                                .put(Properties.File.PATH, filePath)
                                .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
                                .build(),
                              null));

    ETLStage sink1 = new ETLStage(
      "sink1", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                             FileBatchSource.DEFAULT_SCHEMA.toString(),
                                             Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink1"),
                             null));
    ETLStage sink2 = new ETLStage(
      "sink2", new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE,
                             ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                             FileBatchSource.DEFAULT_SCHEMA.toString(),
                                             Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink2"),
                             null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "FileToTPFS");
    runETLOnce(appManager);

    for (String sinkName : new String[] { "fileSink1", "fileSink2" }) {
      DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(sinkName);
      try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
        List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(testData, records.get(0).get("body").toString());
      }
    }
  }

  @Test
  public void testDuplicateStageNameInPipeline() throws Exception {
    String filePath = "file:///tmp/test/text.txt";

    ETLStage source = new ETLStage("source", new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
                                                           ImmutableMap.<String, String>builder()
                                                             .put(Properties.File.FILESYSTEM, "Text")
                                                             .put(Properties.File.PATH, filePath)
                                                             .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
                                                             .build(),
                                                           null));

    ETLStage sink1 = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                            FileBatchSource.DEFAULT_SCHEMA.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink1"),
                            null));
    // duplicate name for 2nd sink, should throw exception
    ETLStage sink2 = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                            FileBatchSource.DEFAULT_SCHEMA.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink2"),
                            null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileToTPFS");

    // deploying would thrown an excpetion
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }
}
