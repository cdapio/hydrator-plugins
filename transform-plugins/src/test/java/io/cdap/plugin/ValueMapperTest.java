/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test case for {@link ValueMapper}.
 */
// Ignoring these tests because the ValueMapper plugin uses the Lookup capability that is only available in MapReduce
// and not Spark. MapReduce engine is deprecated and the mapreduce test dependencies are currently causing
// failures in some test environments.
@Ignore
public class ValueMapperTest extends TransformPluginsTestBase {

  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord",
                    Schema.Field.of(ValueMapperTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.DESIGNATIONID,
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String SALARY = "salary";
  private static final String DESIGNATIONID = "designationid";
  private static final String DESIGNATIONNAME = "designationName";
  private static final String SALARYDESC = "salaryDesc";

  private static final String STAGE = "stage";
  private static final String MOCK_STAGE = "mockstage";

  @Test
  public void testEmptyAndNull() throws Exception {
    String inputTable = "input_table_test_Empty_Null";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_test_Empty_Null:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_test_Empty_Null";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("valuemappertest_test_Empty_Null");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_test_Empty_Null");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_test_Empty_Null");
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
        .set(DESIGNATIONID, "").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "103").set(NAME, "Allie").set(SALARY, "2000")
        .set(DESIGNATIONID, "4").build()
    );

    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<>();
    nameDesignationMap.put("John", "DEFAULTID");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "DEFAULTID");
    nameDesignationMap.put("Allie", "DEFAULTID");

    Assert.assertEquals(4, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(3).get(NAME)), outputRecords.get(3)
      .get(DESIGNATIONNAME));
  }

  @Test
  public void testWithNoDefaults() throws Exception {
    String inputTable = "input_table_without_defaults";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_without_defaults:designationName")
      .put("defaults", "")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_without_defaults";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("valuemappertest_without_defaults");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_without_defaults");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_without_defaults");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1".getBytes(Charsets.UTF_8), "SE".getBytes(Charsets.UTF_8));
    keyValueTable.write("2".getBytes(Charsets.UTF_8), "SSE".getBytes(Charsets.UTF_8));
    keyValueTable.write("3".getBytes(Charsets.UTF_8), "ML".getBytes(Charsets.UTF_8));
    keyValueTable.write("4".getBytes(Charsets.UTF_8), "TL".getBytes(Charsets.UTF_8));
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
        .set(DESIGNATIONID, null).build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
        .set(DESIGNATIONID, "2").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
        .set(DESIGNATIONID, "").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "103").set(NAME, "Allie").set(SALARY, "2000")
        .set(DESIGNATIONID, "4").build());
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Map<String, String> nameDesignationMap = new HashMap<>();
    nameDesignationMap.put("John", null);
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "");
    nameDesignationMap.put("Allie", "TL");

    Map<String, String> nameSalaryMap = new HashMap<>();
    nameSalaryMap.put("John", "1000");
    nameSalaryMap.put("Kerry", "1030");
    nameSalaryMap.put("Mathew", "1230");
    nameSalaryMap.put("Allie", "2000");

    Assert.assertEquals(4, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(3).get(NAME)), outputRecords.get(3)
      .get(DESIGNATIONNAME));

    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(SALARY));
    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(SALARY));
    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(SALARY));
    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(3).get(NAME)), outputRecords.get(3)
      .get(SALARY));
  }

  @Test
  public void testWithMultipleMapping() throws Exception {
    String inputTable = "input_table_with_multi_mapping";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designation_lookup_table_with_multi_mapping:designationName," +
        "salary:salary_lookup_table:salaryDesc")
      .put("defaults", "designationid:DefaultID")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "output_table_with_multi_mapping";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("valuemappertest_with_multi_mapping");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    addDatasetInstance(KeyValueTable.class.getName(), "designation_lookup_table_with_multi_mapping");
    DataSetManager<KeyValueTable> dataSetManager = getDataset("designation_lookup_table_with_multi_mapping");
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

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Map<String, String> nameDesignationMap = new HashMap<>();
    nameDesignationMap.put("John", "SE");
    nameDesignationMap.put("Kerry", "SSE");
    nameDesignationMap.put("Mathew", "ML");

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(DESIGNATIONNAME));
    Assert.assertEquals(nameDesignationMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(DESIGNATIONNAME));

    Map<String, String> nameSalaryMap = new HashMap<>();
    nameSalaryMap.put("John", "Low");
    nameSalaryMap.put("Kerry", "Medium");
    nameSalaryMap.put("Mathew", "High");

    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(0).get(NAME)), outputRecords.get(0)
      .get(SALARYDESC));
    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(1).get(NAME)), outputRecords.get(1)
      .get(SALARYDESC));
    Assert.assertEquals(nameSalaryMap.get(outputRecords.get(2).get(NAME)), outputRecords.get(2)
      .get(SALARYDESC));
  }

  @Test
  public void testStringHandling() throws Exception {
    Schema inputSchema = Schema.recordOf("sourceRecord",
                                           Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.INT)));

    ValueMapper.Config config = new ValueMapper.Config("designationid:designation_lookup_table:designationName",
                                                       "designationid:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new ValueMapper(config).configurePipeline(configurer);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(2, e.getFailures().get(0).getCauses().size());
    }
  }

  @Test
  public void testSchemaHandling() throws Exception {
    Schema inputSchema = Schema.recordOf("sourceRecord",
                                         Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(DESIGNATIONID,
                                                         Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    ValueMapper.Config config = new ValueMapper.Config("designationid:designation_lookup_table:designationName",
                                                       "designationid:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    new ValueMapper(config).configurePipeline(configurer);
    Schema outputSchema = configurer.getOutputSchema();

    Schema expectedOutputSchema = Schema.recordOf("sourceRecord.formatted",
                                         Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(DESIGNATIONNAME, Schema.of(Schema.Type.STRING)));

    Assert.assertEquals(expectedOutputSchema, outputSchema);
  }

  @Test
  public void testMappingValidation() throws Exception {
    Schema inputSchema = Schema.recordOf("sourceRecord",
                                         Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.STRING)));

    ValueMapper.Config config = new ValueMapper.Config("designationid:designation_lookup_table",
                                                       "designationid:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new ValueMapper(config).configurePipeline(configurer);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, ValueMapper.Config.MAPPING);
      expectedCause.addAttribute(CauseAttributes.CONFIG_ELEMENT, "designationid:designation_lookup_table");
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
    }
  }

  @Test
  public void testMappingDoesNotExistInInput() throws Exception {
    Schema inputSchema = Schema.recordOf("sourceRecord",
        Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.STRING)));

    ValueMapper.Config config = new ValueMapper.Config("does_not_exist:designation_lookup_table:designationName",
        "does_not_exist:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    ValueMapper mapper = new ValueMapper(config);
    try {
      mapper.configurePipeline(configurer);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, ValueMapper.Config.MAPPING);
      expectedCause.addAttribute(CauseAttributes.CONFIG_ELEMENT,
                                 "does_not_exist:designation_lookup_table:designationName");
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
    }
  }

  @Test
  public void testDefaultDoesNotExistInMapping() throws Exception {
    Schema inputSchema = Schema.recordOf("sourceRecord",
        Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(SALARY, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DESIGNATIONID, Schema.of(Schema.Type.STRING)));

    ValueMapper.Config config = new ValueMapper.Config("designationid:designation_lookup_table:designationName",
        "does_not_exist:DEFAULTID");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    ValueMapper mapper = new ValueMapper(config);
    try {
      mapper.configurePipeline(configurer);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, ValueMapper.Config.DEFAULTS);
      expectedCause.addAttribute(CauseAttributes.CONFIG_ELEMENT, "does_not_exist:DEFAULTID");
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
    }
  }
}
