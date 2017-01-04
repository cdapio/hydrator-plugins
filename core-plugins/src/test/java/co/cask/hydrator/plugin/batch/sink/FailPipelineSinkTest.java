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


import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FailPipelineSinkTest extends ETLBatchTestBase {

  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord",
                    Schema.Field.of(FailPipelineSinkTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(FailPipelineSinkTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(FailPipelineSinkTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(FailPipelineSinkTest.DESIGNATIONID,
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String SALARY = "salary";
  private static final String DESIGNATIONID = "designationid";

  @Test
  public void testPlugin() throws Exception {
    String inputTable = "input_table_test_Empty_Null";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));
    Map<String, String> map = new HashMap<>();
    ETLStage sink = new ETLStage("sink",
                                      new ETLPlugin("FailPipeline", BatchSink.PLUGIN_TYPE, map, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("test");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }
}
