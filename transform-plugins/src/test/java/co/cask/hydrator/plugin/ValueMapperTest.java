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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.common.MockPipelineConfigurer;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test case for {@link ValueMapper}.
 */

public class ValueMapperTest extends HydratorTestBase {

  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord",
                    Schema.Field.of(ValueMapperTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.DESIGNATIONID,
                                    Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))));

  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String SALARY = "salary";
  private static final String DESIGNATIONID = "designationid";
  private static final String DESIGNATIONNAME = "designationName";
  private static final String SALARYDESC = "salaryDesc";


  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration();

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
          NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
          new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Add the ETL batch artifact and mock plugins.
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);

    // Add our plugins artifact with the ETL batch artifact as its parent.
    // This will make our plugins available to the ETL batch.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("transform-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
            ValueMapper.class, CSVFormat.class, Base64.class);
  }

  @Test
  public void testSimpleFlow() throws Exception {

    Schema inputSchema = Schema.recordOf("sourceRecord",
                                           Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.STRING)));

    String inputTable = "input_table";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set(ID, "100").set(NAME, "John").set(SALARY, "1000").set
        (DESIGNATIONID, "1").build(),
      StructuredRecord.builder(inputSchema).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030").set
        (DESIGNATIONID, "2").build(),
      StructuredRecord.builder(inputSchema).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230").set
        (DESIGNATIONID, "3").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "ML");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2).
      get(DESIGNATIONNAME));

  }

  @Test
  public void testNull() throws Exception {

    String inputTable = "input_table_one";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_one:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_one";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_one");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_one");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_one");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000").
        set(DESIGNATIONID, "1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030").
        set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230").
        set(DESIGNATIONID, null).build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "DEFAULTID");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2).
      get(DESIGNATIONNAME));

  }

  @Test
  public void testEmpty() throws Exception {

    String inputTable = "input_table_two";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_two:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_two";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_two");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_two");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_two");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000").
        set(DESIGNATIONID, "1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030").
        set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230").
        set(DESIGNATIONID, "").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "DEFAULTID");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2).
      get(DESIGNATIONNAME));

  }


  @Test
  public void testEmptyAndNull() throws Exception {

    String inputTable = "input_table_three";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_three:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_three";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_three");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_three");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_three");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000").
        set(DESIGNATIONID, null).build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030").
        set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230").
        set(DESIGNATIONID, "").build()
    );

    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "DEFAULTID");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "DEFAULTID");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1).
      get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2).
      get(DESIGNATIONNAME));

  }


  @Test
  public void testWithNoDefaults() throws Exception {

    String inputTable = "input_table_four";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_four:designationName")
      .put("defaults", "")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_four";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_four");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_four");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_four");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
        .set(DESIGNATIONID, null).build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
        .set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
        .set(DESIGNATIONID, "").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", null);
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));

  }

  @Test
  public void testWithNoLookupValue() throws Exception {

    String inputTable = "input_table_five";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_five:designationName")
      .put("defaults", "designationid:DefaultID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_five";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_five");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_five");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_five");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
        .set(DESIGNATIONID, "1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
        .set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
        .set(DESIGNATIONID, "3").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "DefaultID");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));

  }

  @Test
  public void testWithMultipleMapping() throws Exception {

    String inputTable = "input_table_six";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_six:designationName," +
        "salary:salary_lookup_table:salaryDesc")
      .put("defaults", "designationid:DefaultID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_six";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_six");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_six");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_six");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    addDatasetInstance(KeyValueTable.class.getName(), "salary_lookup_table");
    DataSetManager<KeyValueTable> salaryDataSetManager = getDataset("salary_lookup_table");
    KeyValueTable dsalaryKeyValueTable = salaryDataSetManager.get();
    dsalaryKeyValueTable.write("1000".getBytes(Charsets.UTF_8), "Low".getBytes(Charsets.UTF_8));
    dsalaryKeyValueTable.write("2000".getBytes(Charsets.UTF_8), "Medium".getBytes(Charsets.UTF_8));
    dsalaryKeyValueTable.write("5000".getBytes(Charsets.UTF_8), "High".getBytes(Charsets.UTF_8));
    salaryDataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
        .set(DESIGNATIONID, "1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "2000")
        .set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "5000")
        .set(DESIGNATIONID, "3").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "ML");

    StructuredRecord recordOne = outputRecords.get(0);
    StructuredRecord recordTwo = outputRecords.get(1);
    StructuredRecord recordThree = outputRecords.get(2);

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(recordOne.get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(recordTwo.get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(recordThree.get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));

    Map<String, String> nameSalaryMap = new HashMap<String, String>();
    nameSalaryMap.put("John", "Low");
    nameSalaryMap.put("Kerry", "Medium");
    nameSalaryMap.put("Mathew", "High");


    Assert.assertEquals(nameSalaryMap.get(recordOne.get(NAME)), outputRecords.get(0)
      .get(SALARYDESC));
    Assert.assertEquals(nameSalaryMap.get(recordTwo.get(NAME)), outputRecords.get(1)
      .get(SALARYDESC));
    Assert.assertEquals(nameSalaryMap.get(recordThree.get(NAME)), outputRecords.get(2)
      .get(SALARYDESC));

  }

  @Test
  public void testTargetSchemaValidation() throws Exception {

    String inputTable = "input_table_seven";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_seven:designationName")
      .put("defaults", "designationid:DefaultID")
      .put("name", "FlowPlugin").build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_seven";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "valuemappertest_seven");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_seven");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_seven");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000").set
        (DESIGNATIONID, "1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030").set
        (DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230").set
        (DESIGNATIONID, "3").build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<String, String>();
    nameDesignationMap.put("John", "SE");

    Assert.assertEquals(Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)).getType(),
                        outputRecords.get(0).getSchema().getField(DESIGNATIONNAME).getSchema().getType());

  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringHandling() throws Exception {

    Schema inputSchema = Schema.recordOf("sourceRecord",
                                           Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.INT)));

    ValueMapper.Config config = new ValueMapper.Config("designationid:designation_lookup_table:designationName",
                                                       "designationid:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    new ValueMapper(config).configurePipeline(configurer);

  }

}
