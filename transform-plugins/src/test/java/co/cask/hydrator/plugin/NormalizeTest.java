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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test case for {@link Normalize}.
 */
public class NormalizeTest extends TransformPluginsTestBase {
  private static final String CUSTOMER_ID = "CustomerId";
  private static final String ITEM_ID = "ItemId";
  private static final String ITEM_COST = "ItemCost";
  private static final String PURCHASE_DATE = "PurchaseDate";
  private static final String ID = "Id";
  private static final String DATE = "Date";
  private static final String ATTRIBUTE_TYPE = "AttributeType";
  private static final String ATTRIBUTE_VALUE = "AttributeValue";
  private static final String CUSTOMER_ID_FIRST = "S23424242";
  private static final String CUSTOMER_ID_SECOND = "R45764646";
  private static final String ITEM_ID_ROW1 = "UR-AR-243123-ST";
  private static final String ITEM_ID_ROW2 = "SKU-234294242942";
  private static final String ITEM_ID_ROW3 = "SKU-567757543532";
  private static final String PURCHASE_DATE_ROW1 = "08/09/2015";
  private static final String PURCHASE_DATE_ROW2 = "10/12/2015";
  private static final String PURCHASE_DATE_ROW3 = "06/09/2014";
  private static final double ITEM_COST_ROW1 = 245.67;
  private static final double ITEM_COST_ROW2 = 67.90;
  private static final double ITEM_COST_ROW3 = 14.15;
  private static final Map<String, Object> dataMap = new HashMap<>();
  private static final Schema INPUT_SCHEMA =
    Schema.recordOf("inputSchema",
                    Schema.Field.of(CUSTOMER_ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ITEM_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of(ITEM_COST, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of(PURCHASE_DATE, Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("outputSchema",
                    Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(DATE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ATTRIBUTE_TYPE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ATTRIBUTE_VALUE, Schema.of(Schema.Type.STRING)));

  private static String validFieldMapping;
  private static String validFieldNormalizing;

  @BeforeClass
  public static void initialiseData() {
    dataMap.put(CUSTOMER_ID_FIRST + PURCHASE_DATE_ROW1 + ITEM_ID, ITEM_ID_ROW1);
    dataMap.put(CUSTOMER_ID_FIRST + PURCHASE_DATE_ROW2 + ITEM_ID, ITEM_ID_ROW2);
    dataMap.put(CUSTOMER_ID_SECOND + PURCHASE_DATE_ROW3 + ITEM_ID, ITEM_ID_ROW3);

    dataMap.put(CUSTOMER_ID_FIRST + PURCHASE_DATE_ROW1 + ITEM_COST, String.valueOf(ITEM_COST_ROW1));
    dataMap.put(CUSTOMER_ID_FIRST + PURCHASE_DATE_ROW2 + ITEM_COST, String.valueOf(ITEM_COST_ROW2));
    dataMap.put(CUSTOMER_ID_SECOND + PURCHASE_DATE_ROW3 + ITEM_COST, String.valueOf(ITEM_COST_ROW3));

    validFieldMapping = CUSTOMER_ID + ":" + ID + "," + PURCHASE_DATE + ":" + DATE;
    validFieldNormalizing = ITEM_ID + ":" + ATTRIBUTE_TYPE + ":" + ATTRIBUTE_VALUE + "," + ITEM_COST + ":"
      + ATTRIBUTE_TYPE + ":" + ATTRIBUTE_VALUE;
  }

  private String getKeyFromRecord(StructuredRecord record) {
    return record.get(ID).toString() + record.get(DATE) + record.get(ATTRIBUTE_TYPE);
  }

  private ApplicationManager deployApplication(Map<String, String> sourceProperties, String inputDatasetName,
                                               String outputDatasetName, String applicationName) throws Exception {
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));
    ETLStage transform = new ETLStage("normalize",
                                      new ETLPlugin("Normalize", Transform.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId, appRequest);
  }

  private void startWorkflow(ApplicationManager appManager, ProgramRunStatus status) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(status, 1, 5, TimeUnit.MINUTES);
  }

  @Test
  public void testOutputSchema() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping, validFieldNormalizing,
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
    Assert.assertEquals(OUTPUT_SCHEMA, configurer.getOutputSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFieldMapping() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(null, validFieldNormalizing,
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFieldNormalizing() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping, null,
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyOutputSchema() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping, validFieldNormalizing, null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMappingValues() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig("CustomerId,PurchaseDate:Date",
                                                                     validFieldNormalizing, OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNormalizingValues() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping,
                                                                     "ItemId:AttributeType," +
                                                                       "ItemCost:AttributeType:AttributeValue",
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOutputSchema() throws Exception {
    //schema with no ID field
    Schema outputSchema =
      Schema.recordOf("outputSchema",
                      Schema.Field.of(DATE, Schema.of(Schema.Type.STRING)),
                      Schema.Field.of(ATTRIBUTE_TYPE, Schema.of(Schema.Type.STRING)),
                      Schema.Field.of(ATTRIBUTE_VALUE, Schema.of(Schema.Type.STRING)));
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping, validFieldNormalizing,
                                                                     outputSchema.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOutputSchemaFieldType() throws Exception {
    //schema with ID field as long
    Schema outputSchema =
      Schema.recordOf("outputSchema",
                      Schema.Field.of(ID, Schema.of(Schema.Type.LONG)),
                      Schema.Field.of(DATE, Schema.of(Schema.Type.STRING)),
                      Schema.Field.of(ATTRIBUTE_TYPE, Schema.of(Schema.Type.STRING)),
                      Schema.Field.of(ATTRIBUTE_VALUE, Schema.of(Schema.Type.STRING)));
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping, validFieldNormalizing,
                                                                     outputSchema.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMappingsFromInputSchema() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig("Purchaser:Id,PurchaseDate:Date",
                                                                     validFieldNormalizing, OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNormalizingFromInputSchema() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping,
                                                                     "ObjectId:AttributeType:AttributeValue," +
                                                                       "ItemCost:AttributeType:AttributeValue",
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNormalizeTypeAndValue() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig(validFieldMapping,
                                                                     "ItemId:AttributeType:AttributeValue," +
                                                                       "ItemCost:ExpenseType:ExpenseValue",
                                                                     OUTPUT_SCHEMA.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Ignore
  @Test
  public void testNormalize() throws Exception {
    String inputTable = "inputNormalizeTable";
    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("fieldMapping", validFieldMapping)
      .put("fieldNormalizing", validFieldNormalizing)
      .put("outputSchema", OUTPUT_SCHEMA.toString())
      .build();
    String outputTable = "outputNormalizeTable";
    ApplicationManager applicationManager = deployApplication(sourceproperties, inputTable, outputTable,
                                                              "normalizeTest");
    
    DataSetManager<Table> inputManager = getDataset(inputTable);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, ITEM_ID_ROW1).set(CUSTOMER_ID, CUSTOMER_ID_FIRST)
        .set(ITEM_COST, ITEM_COST_ROW1).set(PURCHASE_DATE, PURCHASE_DATE_ROW1).build(),
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, ITEM_ID_ROW2).set(CUSTOMER_ID, CUSTOMER_ID_FIRST)
        .set(ITEM_COST, ITEM_COST_ROW2).set(PURCHASE_DATE, PURCHASE_DATE_ROW2).build(),
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, ITEM_ID_ROW3).set(CUSTOMER_ID, CUSTOMER_ID_SECOND)
        .set(ITEM_COST, ITEM_COST_ROW3).set(PURCHASE_DATE, PURCHASE_DATE_ROW3).build()
    );
    MockSource.writeInput(inputManager, input);

    startWorkflow(applicationManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(6, outputRecords.size());
    Assert.assertEquals(outputRecords.get(0).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(0))));
    Assert.assertEquals(outputRecords.get(1).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(1))));
    Assert.assertEquals(outputRecords.get(2).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(2))));
    Assert.assertEquals(outputRecords.get(3).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(3))));
    Assert.assertEquals(outputRecords.get(4).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(4))));
    Assert.assertEquals(outputRecords.get(5).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(5))));
  }

  @Ignore
  @Test
  public void testNormalizeWithEmptyAttributeValue() throws Exception {
    String inputTable = "inputNormalizeWithEmptyValueTable";
    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("fieldMapping", validFieldMapping)
      .put("fieldNormalizing", validFieldNormalizing)
      .put("outputSchema", OUTPUT_SCHEMA.toString())
      .build();
    String outputTable = "outputNormalizeWithEmptyValueTable";
    ApplicationManager applicationManager = deployApplication(sourceproperties, inputTable, outputTable,
                                                              "normalizeWithEmptyValueTest");

    DataSetManager<Table> inputManager = getDataset(inputTable);

    //ItemId for first row and ItemCost for second row is null.
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, null).set(CUSTOMER_ID, CUSTOMER_ID_FIRST)
        .set(ITEM_COST, ITEM_COST_ROW1).set(PURCHASE_DATE, PURCHASE_DATE_ROW1).build(),
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, ITEM_ID_ROW2).set(CUSTOMER_ID, CUSTOMER_ID_FIRST)
        .set(ITEM_COST, null).set(PURCHASE_DATE, PURCHASE_DATE_ROW2).build(),
      StructuredRecord.builder(INPUT_SCHEMA).set(ITEM_ID, ITEM_ID_ROW3).set(CUSTOMER_ID, CUSTOMER_ID_SECOND)
        .set(ITEM_COST, ITEM_COST_ROW3).set(PURCHASE_DATE, PURCHASE_DATE_ROW3).build()
    );
    MockSource.writeInput(inputManager, input);

    startWorkflow(applicationManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    //there should be 4 records only, null value record must not emit.
    Assert.assertEquals(4, outputRecords.size());
    Assert.assertEquals(outputRecords.get(0).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(0))));
    Assert.assertEquals(outputRecords.get(1).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(1))));
    Assert.assertEquals(outputRecords.get(2).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(2))));
    Assert.assertEquals(outputRecords.get(3).get(ATTRIBUTE_VALUE), dataMap.get(getKeyFromRecord(outputRecords.get(3))));
  }
}
