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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.batch.source.FileBatchSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTestRun extends ETLBatchTestBase {
  public static final Schema ERROR_SCHEMA = Schema.recordOf(
    "error",
    Schema.Field.of("errCode", Schema.of(Schema.Type.INT)),
    Schema.Field.of("errMsg", Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                        Schema.of(Schema.Type.NULL))),
    Schema.Field.of("invalidRecord", Schema.of(Schema.Type.STRING))
  );

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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

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
                                                                 ImmutableMap.<String, String>of(), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("kvTable1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 10000; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> table2 = getDataset("kvTable2");
    try (KeyValueTable outputTable = table2.get()) {
      for (int i = 0; i < 10000; i++) {
        Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
      }
    }
  }

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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "DagApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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


    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

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
                                    "validationScript", validationScript),
                    null),
      "keyErrors");

    ETLStage sink = new ETLStage(
      "sink", new ETLPlugin("Table",
                            BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(
                              Properties.BatchReadableWritable.NAME, "outputTable",
                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                              Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                            null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TableToTable");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("user"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertEquals(0, row.getColumns().size());

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("keyErrors");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, ERROR_SCHEMA);
      Assert.assertEquals(1, records.size());
    }
  }

  @Test
  public void testS3toTPFS() throws Exception {
    String testPath = "s3n://test/";
    String testFile1 = "2015-06-17-00-00-00.txt";
    String testData1 = "Sample data for testing.";

    String testFile2 = "abc.txt";
    String testData2 = "Sample data for testing.";

    S3NInMemoryFileSystem fs = new S3NInMemoryFileSystem();
    Configuration conf = new Configuration();
    conf.set("fs.s3n.impl", S3NInMemoryFileSystem.class.getName());
    fs.initialize(URI.create("s3n://test/"), conf);
    fs.createNewFile(new Path(testPath));

    try (FSDataOutputStream fos1 = fs.create(new Path(testPath + testFile1))) {
      fos1.write(testData1.getBytes());
      fos1.flush();
    }

    try (FSDataOutputStream fos2 = fs.create(new Path(testPath + testFile2))) {
      fos2.write(testData2.getBytes());
      fos2.flush();
    }

    Method method = FileSystem.class.getDeclaredMethod("addFileSystemForTesting",
                                                       URI.class, Configuration.class, FileSystem.class);
    method.setAccessible(true);
    method.invoke(FileSystem.class, URI.create("s3n://test/"), conf, fs);
    ETLStage source = new ETLStage(
      "source",
      new ETLPlugin("S3", BatchSource.PLUGIN_TYPE,
                    ImmutableMap.<String, String>builder()
                      .put(Constants.Reference.REFERENCE_NAME, "S3TestSource")
                      .put(Properties.S3.ACCESS_KEY, "key")
                      .put(Properties.S3.ACCESS_ID, "ID")
                      .put(Properties.S3.PATH, testPath)
                      .put(Properties.S3.FILE_REGEX, "abc.*")
                      .build(),
                    null));
    ETLStage sink = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                            FileBatchSource.DEFAULT_SCHEMA.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, "TPFSsink"),
                            null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "S3ToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("TPFSsink");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
      // Two input files, each with one input record were specified. However, only one file matches the regex,
      // so only one record should be found in the output.
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(testData1, records.get(0).get("body").toString());
    }
  }

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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    for (String sinkName : new String[] { "fileSink1", "fileSink2" }) {
      DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(sinkName);
      try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
        List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(testData, records.get(0).get("body").toString());
      }
    }
  }

  @Test(expected = Exception.class)
  public void testDuplicateStageNameInPipeline() throws Exception {
    String filePath = "file:///tmp/test/text.txt";

    ETLStage source = new ETLStage("source", new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
                                                           ImmutableMap.<String, String>builder()
                                                             .put(Properties.File.FILESYSTEM, "Text")
                                                             .put(Properties.File.PATH, filePath)
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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToTPFS");

    // deploying would thrown an excpetion
    deployApplication(appId, appRequest);
  }
}
