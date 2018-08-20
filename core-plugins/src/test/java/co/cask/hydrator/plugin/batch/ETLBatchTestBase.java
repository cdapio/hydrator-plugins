/*
 * Copyright © 2015, 2016 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.alert.TMSAlertPublisher;
import co.cask.hydrator.plugin.batch.action.EmailAction;
import co.cask.hydrator.plugin.batch.action.SSHAction;
import co.cask.hydrator.plugin.batch.aggregator.DedupAggregator;
import co.cask.hydrator.plugin.batch.aggregator.GroupByAggregator;
import co.cask.hydrator.plugin.batch.joiner.Joiner;
import co.cask.hydrator.plugin.batch.sink.BatchCubeSink;
import co.cask.hydrator.plugin.batch.sink.FileSink;
import co.cask.hydrator.plugin.batch.sink.KVTableSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchAvroSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchParquetSink;
import co.cask.hydrator.plugin.batch.sink.TableSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDataSetORCSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetParquetSink;
import co.cask.hydrator.plugin.batch.source.FTPBatchSource;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchAvroSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchParquetSource;
import co.cask.hydrator.plugin.batch.source.StreamBatchSource;
import co.cask.hydrator.plugin.batch.source.TableSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetParquetSource;
import co.cask.hydrator.plugin.error.ErrorCollector;
import co.cask.hydrator.plugin.transform.JavaScriptTransform;
import co.cask.hydrator.plugin.transform.ProjectionTransform;
import co.cask.hydrator.plugin.transform.PythonEvaluator;
import co.cask.hydrator.plugin.transform.StructuredRecordToGenericRecordTransform;
import co.cask.hydrator.plugin.transform.ValidatorTransform;
import co.cask.hydrator.plugin.validator.CoreValidator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.twill.filesystem.Location;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base test class that sets up plugin and the etl batch app artifacts.
 */
public class ETLBatchTestBase extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "1.0.0"), parents,
                      KVTableSource.class,
                      StreamBatchSource.class,
                      TableSource.class,
                      TimePartitionedFileSetDatasetAvroSource.class,
                      TimePartitionedFileSetDatasetParquetSource.class, AvroParquetInputFormat.class,
                      BatchCubeSink.class, KVTableSink.class, TableSink.class,
                      TimePartitionedFileSetDatasetAvroSink.class, AvroKeyOutputFormat.class, AvroKey.class,
                      TimePartitionedFileSetDatasetParquetSink.class, AvroParquetOutputFormat.class,
                      TimePartitionedFileSetDataSetORCSink.class, OrcStruct.class,
                      OrcMapreduceRecordWriter.class, TimestampColumnVector.class,
                      SnapshotFileBatchAvroSink.class, SnapshotFileBatchParquetSink.class,
                      SnapshotFileBatchAvroSource.class, SnapshotFileBatchParquetSource.class,
                      FTPBatchSource.class,
                      ProjectionTransform.class,
                      ValidatorTransform.class, CoreValidator.class,
                      StructuredRecordToGenericRecordTransform.class,
                      JavaScriptTransform.class,
                      PythonEvaluator.class,
                      GroupByAggregator.class,
                      DedupAggregator.class,
                      Joiner.class,
                      EmailAction.class,
                      SSHAction.class,
                      TMSAlertPublisher.class,
                      ErrorCollector.class,
                      FileSink.class);
  }

  protected List<GenericRecord> readOutput(TimePartitionedFileSet fileSet, Schema schema) throws IOException {
    List<GenericRecord> records = Lists.newArrayList();
    for (Location dayLoc : fileSet.getEmbeddedFileSet().getBaseLocation().list()) {
      // this level should be the day (ex: 2015-01-19)
      for (Location timeLoc : dayLoc.list()) {
        // this level should be the time (ex: 21-23.1234567890000)
        records.addAll(readOutput(timeLoc, schema));
      }
    }
    return records;
  }

  protected List<GenericRecord> readOutput(Location location, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records = Lists.newArrayList();
    for (Location file : location.list()) {
      // this level should be the actual mapred output
      String locName = file.getName();

      if (locName.endsWith(".avro")) {
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
        Iterables.addAll(records, fileStream);
        fileStream.close();
      }
      if (locName.endsWith(".parquet")) {
        Path parquetFile = new Path(file.toString());
        AvroParquetReader.Builder<GenericRecord> genericRecordBuilder = AvroParquetReader.builder(parquetFile);
        ParquetReader<GenericRecord> reader = genericRecordBuilder.build();
        GenericRecord result = reader.read();
        while (result != null) {
          records.add(result);
          result = reader.read();
        }
      }
    }
    return records;
  }

  protected ApplicationManager deployETL(ETLBatchConfig etlConfig, String appName) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager)
    throws TimeoutException, InterruptedException, ExecutionException {
    return runETLOnce(appManager, ImmutableMap.of());
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   * @param arguments the arguments to be passed when running SmartWorkflow
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager, Map<String, String> arguments)
    throws TimeoutException, InterruptedException, ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    int numRuns = workflowManager.getHistory().size();
    workflowManager.start(arguments);
    Tasks.waitFor(numRuns + 1, () -> workflowManager.getHistory().size(), 20, TimeUnit.SECONDS);
    workflowManager.waitForStopped(5, TimeUnit.MINUTES);
    return workflowManager;
  }
}
