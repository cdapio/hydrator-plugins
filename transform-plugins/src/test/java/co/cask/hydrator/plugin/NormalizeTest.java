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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.common.MockPipelineConfigurer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test case for {@link Normalize}.
 */
public class NormalizeTest extends TransformPluginsTestBase {

  private static final String CUSTOMER_ID = "CustomerId";
  private static final String PURCHASE_DATE = "PurchaseDate";
  private static final String ITEM_ID = "ItemId";
  private static final String ITEM_COST = "ItemCost";
  private static final Schema SOURCE_SCHEMA = Schema.recordOf("sourceRecord",
                                                              Schema.Field.of(CUSTOMER_ID,
                                                                             Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of(ITEM_ID, Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of(ITEM_COST, Schema.of(Schema.Type.FLOAT)),
                                                              Schema.Field.of(PURCHASE_DATE,
                                                                             Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT_SCHEMA = Schema.recordOf("outputRecord",
                                                              Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of("Date", Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of("AttributeType",
                                                                              Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of("AttributeValue",
                                                                              Schema.of(Schema.Type.STRING)));

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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId.toId(), appRequest);
  }

  private void startMapReduceJob(ApplicationManager appManager) throws Exception {
    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  @Test
  public void testOutputSchema() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig("CustomerId:Id,Purchase Date:Date",
                                                                     "ItemId:AttributeType:AttributeValue," +
                                                                       "ItemCost:AttributeType:AttributeValue");



    MockPipelineConfigurer configurer = new MockPipelineConfigurer(SOURCE_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
    Assert.assertEquals(OUTPUT_SCHEMA, configurer.getOutputSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMappingValues() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig("CustomerId,Purchase Date:Date",
                                                                     "ItemId:AttributeType:AttributeValue, " +
                                                                       "ItemCost:AttributeType:AttributeValue");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(SOURCE_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNormalizingValues() throws Exception {
    Normalize.NormalizeConfig config = new Normalize.NormalizeConfig("CustomerId:Id,Purchase Date:Date",
                                                                     "ItemId:AttributeType, " +
                                                                       "ItemCost:AttributeType:AttributeValue");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(SOURCE_SCHEMA);
    new Normalize(config).configurePipeline(configurer);
  }

  @Test
  public void testNormalize() throws Exception {
    String inputTable = "inputNormalizeTable";
    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("fieldMapping", CUSTOMER_ID + ":Id," + PURCHASE_DATE + ":Date")
      .put("fieldNormalizing", ITEM_ID + ":AttributeType:AttributeValue," + ITEM_COST + ":AttributeType:AttributeValue")
      .build();
    String outputTable = "outputNormalizeTable";
    ApplicationManager applicationManager = deployApplication(sourceproperties, inputTable, outputTable,
                                                              "normalizeTest");
    
    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA).set(ITEM_ID, "UR-AR-243123-ST").set(CUSTOMER_ID, "S23424242")
        .set(ITEM_COST, 245.67).set(PURCHASE_DATE, "08/09/2015").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ITEM_ID, "SKU-234294242942").set(CUSTOMER_ID, "S23424242")
        .set(ITEM_COST, 67.90).set(PURCHASE_DATE, "10/12/2015").build(),
      StructuredRecord.builder(SOURCE_SCHEMA).set(ITEM_ID, "SKU-567757543532").set(CUSTOMER_ID, "R45764646")
        .set(ITEM_COST, 14.15).set(PURCHASE_DATE, "06/09/2014").build()
    );
    MockSource.writeInput(inputManager, input);

    startMapReduceJob(applicationManager);

    DataSetManager<Table> outputManager = getDataset(outputTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(6, outputRecords.size());
    Assert.assertEquals("UR-AR-243123-ST", outputRecords.get(0).get(ITEM_ID));
    Assert.assertEquals("245.67", outputRecords.get(1).get(ITEM_COST));
    Assert.assertEquals("SKU-234294242942", outputRecords.get(2).get(ITEM_ID));
    Assert.assertEquals("67.90", outputRecords.get(3).get(ITEM_COST));
    Assert.assertEquals("SKU-567757543532", outputRecords.get(4).get(ITEM_ID));
    Assert.assertEquals("14.15", outputRecords.get(5).get(ITEM_COST));
  }
}
