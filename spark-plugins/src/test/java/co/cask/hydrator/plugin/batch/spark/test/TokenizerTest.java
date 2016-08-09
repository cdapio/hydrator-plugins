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

package co.cask.hydrator.plugin.batch.spark.test;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.spark.Tokenizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for Tokenizer plugin.
 */
public class TokenizerTest extends HydratorTestBase {

    @ClassRule
    public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

    protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
            NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
    protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

    private static final String MULTIPLE = "MultipleColumns";
    private static final String SINGLE = "SingleColumn";
    private static final String OUTPUT_COLUMN = "words";
    private static final String COLUMN_TOKENIZED = "sentence";
    private static final String DELIMITER = "/";
    private static final String SENTENCE1 = "Cask Data /Application Platform";
    private static final String SENTENCE2 = "Cask Hydrator/ is webbased tool";
    private static final String SENTENCE3 = "Hydrator Studio is visual /development environment";
    private static final String SENTENCE4 = "Hydrator plugins /are customizable modules";

    private static final Schema SOURCE_SCHEMA_SINGLE = Schema.recordOf("sourceRecord",
            Schema.Field.of("sentence", Schema.of(Schema.Type.STRING))
    );
    private static final Schema SOURCE_SCHEMA_MULTIPLE = Schema.recordOf("sourceRecord",
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("sentence", Schema.of(Schema.Type.STRING))
    );

    @BeforeClass
    public static void setupTest() throws Exception {
        // add the artifact for etl batch app
        setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
        // add artifact for spark plugins
        addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                Tokenizer.class);
    }

    private ETLBatchConfig buildETLBatchConfig(String text, String dataSetType) {
        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(new ETLStage("source", MockSource.getPlugin(text)))
                .addStage(new ETLStage("sparkcompute",
                        new ETLPlugin(Tokenizer.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,

                                ImmutableMap.of("outputColumn", OUTPUT_COLUMN,
                                        "columnToBeTokenized", COLUMN_TOKENIZED,
                                        "delimiter", DELIMITER),
                                null))).addStage(new ETLStage("sink", MockSink.getPlugin(dataSetType)))
                .addConnection("source", "sparkcompute")
                .addConnection("sparkcompute", "sink")
                .build();
        return etlConfig;
    }

    @Test
    public void testMultiColumnSource() throws Exception {
        String textForMultiple = "textForMultiple";
    /*
     * source --> sparkcompute --> sink
     */
        ETLBatchConfig etlConfig = buildETLBatchConfig(textForMultiple, MULTIPLE);
        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app("TokenizerTest");
        ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
        DataSetManager<Table> inputManager = getDataset(textForMultiple);
        List<StructuredRecord> input = ImmutableList.of(
                StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set("sentence",
                        SENTENCE1).set("name", "CDAP").build(),
                StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set("sentence",
                        SENTENCE2).set("name", "Hydrator").build(),
                StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set("sentence",
                        SENTENCE3).set("name", "Studio").build(),
                StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set("sentence",
                        SENTENCE4).set("name", "Plugins").build()
        );
        MockSource.writeInput(inputManager, input);
        // manually trigger the pipeline
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(5, TimeUnit.MINUTES);
        DataSetManager<Table> tokenizedTexts = getDataset(MULTIPLE);
        List<StructuredRecord> output = MockSink.readOutput(tokenizedTexts);
        // Verify data
        StructuredRecord row1 = output.get(0);
        Assert.assertEquals(4, output.size());
        Assert.assertEquals(1, row1.getSchema().getFields().size());
        Assert.assertEquals("ARRAY", row1.getSchema().getField("words").getSchema().getType().toString());
    }

    @Test
    public void testSingleColumnSource() throws Exception {
        String text = "Single";
    /*
     * source --> sparkcompute --> sink
     */
        ETLBatchConfig etlConfig = buildETLBatchConfig(text, SINGLE);
        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app("TokenizerTest");
        ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
        DataSetManager<Table> inputManager = getDataset(text);
        List<StructuredRecord> input = ImmutableList.of(
                StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set("sentence",
                        SENTENCE1).build(),
                StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set("sentence",
                        SENTENCE2).build(),
                StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set("sentence",
                        SENTENCE3).build(),
                StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set("sentence",
                        SENTENCE4).build()
        );
        MockSource.writeInput(inputManager, input);
        // manually trigger the pipeline
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(5, TimeUnit.MINUTES);
        DataSetManager<Table> tokenizedTextSingle = getDataset(SINGLE);
        List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
        // Verify data
        StructuredRecord row1 = output.get(0);
        Assert.assertEquals(4, output.size());
        Assert.assertEquals(1, row1.getSchema().getFields().size());
        Assert.assertEquals("ARRAY", row1.getSchema().getField("words").getSchema().getType().toString());
    }
}
