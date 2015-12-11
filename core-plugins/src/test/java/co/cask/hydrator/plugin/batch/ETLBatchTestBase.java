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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.plugin.batch.sink.BatchCubeSink;
import co.cask.hydrator.plugin.batch.sink.DBSink;
import co.cask.hydrator.plugin.batch.sink.KVTableSink;
import co.cask.hydrator.plugin.batch.sink.S3AvroBatchSink;
import co.cask.hydrator.plugin.batch.sink.S3ParquetBatchSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchAvroSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchParquetSink;
import co.cask.hydrator.plugin.batch.sink.TableSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetParquetSink;
import co.cask.hydrator.plugin.batch.source.DBSource;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchAvroSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchParquetSource;
import co.cask.hydrator.plugin.batch.source.StreamBatchSource;
import co.cask.hydrator.plugin.batch.source.TableSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetParquetSource;
import co.cask.hydrator.plugin.common.db.DBRecord;
import co.cask.hydrator.plugin.transform.ProjectionTransform;
import co.cask.hydrator.plugin.transform.ScriptFilterTransform;
import co.cask.hydrator.plugin.transform.StructuredRecordToGenericRecordTransform;
import co.cask.hydrator.plugin.transform.ValidatorTransform;
import co.cask.hydrator.plugin.validator.CoreValidator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.api.WriteSupport;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Base test class that sets up plugin and the etl batch app artifacts.
 */
public class ETLBatchTestBase extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "etlbatch", "3.2.0");
  protected static final ArtifactSummary ETLBATCH_ARTIFACT = new ArtifactSummary("etlbatch", "3.2.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    // add the artifact for etl batch app
    addAppArtifact(APP_ARTIFACT_ID, ETLBatchApplication.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName(),
                   "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic", "org.apache.avro.io",
                   // these are not real exports for the application, but are required for unit tests.
                   // the stupid hive-exec jar pulled in by cdap-unit-test contains ParquetInputSplit...
                   // without this, different classloaders will be used for ParquetInputSplit and we'll see errors
                   "parquet.hadoop.api", "parquet.hadoop", "parquet.schema", "parquet.io.api");

    // add artifact for batch sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      DBSource.class, KVTableSource.class, StreamBatchSource.class, TableSource.class, DBRecord.class,
                      TimePartitionedFileSetDatasetAvroSource.class,
                      TimePartitionedFileSetDatasetParquetSource.class,
                      AvroParquetInputFormat.class,
                      BatchCubeSink.class, DBSink.class, KVTableSink.class, TableSink.class,
                      TimePartitionedFileSetDatasetAvroSink.class, AvroKeyOutputFormat.class, AvroKey.class,
                      TimePartitionedFileSetDatasetParquetSink.class, AvroParquetOutputFormat.class,
                      SnapshotFileBatchAvroSink.class, SnapshotFileBatchParquetSink.class,
                      SnapshotFileBatchAvroSource.class, SnapshotFileBatchParquetSource.class,
                      S3AvroBatchSink.class, S3ParquetBatchSink.class);
    // add artifact for transforms
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "transforms", "1.0.0"), APP_ARTIFACT_ID,
                      ProjectionTransform.class, ScriptFilterTransform.class,
                      ValidatorTransform.class, CoreValidator.class,
                      StructuredRecordToGenericRecordTransform.class);

    // add hypersql 3rd party plugin
    PluginClass hypersql = new PluginClass("jdbc", "hypersql", "hypersql jdbc driver", JDBCDriver.class.getName(),
                                           null, Collections.<String, PluginPropertyField>emptyMap());
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "hsql-jdbc", "1.0.0"), APP_ARTIFACT_ID,
                      Sets.newHashSet(hypersql), JDBCDriver.class);
  }

  protected List<GenericRecord> readOutput(TimePartitionedFileSet fileSet, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records = Lists.newArrayList();
    for (Location dayLoc : fileSet.getEmbeddedFileSet().getBaseLocation().list()) {
      // this level should be the day (ex: 2015-01-19)
      for (Location timeLoc : dayLoc.list()) {
        // this level should be the time (ex: 21-23.1234567890000)
        for (Location file : timeLoc.list()) {
          // this level should be the actual mapred output
          String locName = file.getName();

          if (locName.endsWith(".avro")) {
            DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
            Iterables.addAll(records, fileStream);
            fileStream.close();
          }
          if (locName.endsWith(".parquet")) {
            Path parquetFile = new Path(file.toString());
            AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(parquetFile);
            GenericRecord result = reader.read();
            while (result != null) {
              records.add(result);
              result = reader.read();
            }
          }
        }
      }
    }
    return records;
  }
}
