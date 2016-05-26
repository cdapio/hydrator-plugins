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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
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
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.batch.CopybookSource;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link CopybookSource} classes.
 */
public class CopybookTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String cblContents = "000100*                                                                         \n" +
    "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
    "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
    "000400*                                                                         \n" +
    "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
    "000600*                                                                         \n" +
    "000700*   RECORD LENGTH IS 27.                                                  \n" +
    "000800*                                                                         \n" +
    "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
    "001000            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
    "001100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
    "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3.               \n" +
    "001300        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               \n" +
    "001400        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               \n" +
    "001500        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.";


  @BeforeClass
  public static void setupTest() throws Exception {

    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);

    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("copybookreader-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      CopybookSource.class);

    FileInputFormat.setInputPaths(new JobConf(), new Path("src/test/resources"));
  }

  @Test
  public void testCopybookReaderWithRequiredFields() throws Exception {

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("DTAR020-KEYCODE-NO", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                        STRING))),
                                    Schema.Field.of("DTAR020-DATE", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                  DOUBLE))),
                                    Schema.Field.of("DTAR020-DEPT-NO", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                     DOUBLE))),
                                    Schema.Field.of("DTAR020-QTY-SOLD", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                      DOUBLE))),
                                    Schema.Field.of("DTAR020-SALE-PRICE", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                        DOUBLE))));

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", "src/test/resources/DTAR020_FB.bin")
      .put("copybookContents", cblContents)
      .put("drop", "DTAR020-STORE-NO")
      .build();

    ETLStage source = new ETLStage("CopybookReader", new ETLPlugin("CopybookReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CopybookReaderTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());

    Map<String, Double> result = new HashMap<>();
    result.put((String) output.get(0).get("DTAR020-KEYCODE-NO"), (Double) output.get(0).get("DTAR020-SALE-PRICE"));
    result.put((String) output.get(1).get("DTAR020-KEYCODE-NO"), (Double) output.get(1).get("DTAR020-SALE-PRICE"));

    Assert.assertEquals(4.87, result.get("63604808").doubleValue(), 0.1);
    Assert.assertEquals(5.01, result.get("69694158").doubleValue(), 0.1);
    Assert.assertEquals("Expected schema", output.get(0).getSchema(), schema);

  }

  @Test
  public void testCopybookReaderWithAllFields() throws Exception {

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("DTAR020-KEYCODE-NO", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                        STRING))),
                                    Schema.Field.of("DTAR020-STORE-NO", Schema.nullableOf(Schema.of(Schema.Type
                                                                                                      .DOUBLE))),
                                    Schema.Field.of("DTAR020-DATE", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                  DOUBLE))),
                                    Schema.Field.of("DTAR020-DEPT-NO", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                     DOUBLE))),
                                    Schema.Field.of("DTAR020-QTY-SOLD", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                      DOUBLE))),
                                    Schema.Field.of("DTAR020-SALE-PRICE", Schema.nullableOf(Schema.of(Schema.Type.
                                                                                                        DOUBLE))));

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", "src/test/resources/DTAR020_FB.bin")
      .put("copybookContents", cblContents)
      .put("maxSplitSize", "5")
      .build();

    ETLStage source = new ETLStage("CopybookReader", new ETLPlugin("CopybookReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsource-test-wihtout-schema";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CopybookReaderTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());

    Map<String, Double> result = new HashMap<>();
    result.put((String) output.get(0).get("DTAR020-KEYCODE-NO"), (Double) output.get(0).get("DTAR020-SALE-PRICE"));
    result.put((String) output.get(1).get("DTAR020-KEYCODE-NO"), (Double) output.get(1).get("DTAR020-SALE-PRICE"));

    Assert.assertEquals(4.87, result.get("63604808").doubleValue(), 0.1);
    Assert.assertEquals(5.01, result.get("69694158").doubleValue(), 0.1);
    Assert.assertEquals("Expected schema", output.get(0).getSchema(), schema);
  }

  @Test
  public void testInvalidCopybookReaderSource() throws Exception {

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", "src/test/resources/DTAR020_FB.txt")
      .build();

    ETLStage source = new ETLStage("CopybookReader", new ETLPlugin("CopybookReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsource-test-incorrect-schema";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CopybookReaderTest");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (IllegalStateException e) {
      // expected - since cobol copybook is required
    }
  }

  @Test
  public void testDefaults() {
    CopybookSource.CopybookSourceConfig copybookSourceConfig = new CopybookSource.CopybookSourceConfig();
    Assert.assertEquals(Long.toString(CopybookSource.DEFAULT_MAX_SPLIT_SIZE_IN_MB * 1024 * 1024),
                        copybookSourceConfig.getMaxSplitSize().toString());
  }
}


