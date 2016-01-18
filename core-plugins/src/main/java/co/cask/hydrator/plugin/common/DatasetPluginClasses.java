/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.common;

import co.cask.hydrator.plugin.batch.sink.BatchCubeSink;
import co.cask.hydrator.plugin.batch.sink.KVTableSink;
import co.cask.hydrator.plugin.batch.sink.S3AvroBatchSink;
import co.cask.hydrator.plugin.batch.sink.S3ParquetBatchSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchAvroSink;
import co.cask.hydrator.plugin.batch.sink.SnapshotFileBatchParquetSink;
import co.cask.hydrator.plugin.batch.sink.TableSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetParquetSink;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchAvroSource;
import co.cask.hydrator.plugin.batch.source.SnapshotFileBatchParquetSource;
import co.cask.hydrator.plugin.batch.source.StreamBatchSource;
import co.cask.hydrator.plugin.batch.source.TableSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetParquetSource;
import co.cask.hydrator.plugin.realtime.sink.RealtimeCubeSink;
import co.cask.hydrator.plugin.realtime.sink.RealtimeTableSink;
import co.cask.hydrator.plugin.realtime.sink.StreamSink;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import co.cask.hydrator.plugin.realtime.source.JmsSource;
import co.cask.hydrator.plugin.realtime.source.SqsSource;
import co.cask.hydrator.plugin.realtime.source.TwitterSource;
import co.cask.hydrator.plugin.transform.ProjectionTransform;
import co.cask.hydrator.plugin.transform.ScriptFilterTransform;
import co.cask.hydrator.plugin.transform.ScriptTransform;
import co.cask.hydrator.plugin.transform.StructuredRecordToGenericRecordTransform;
import co.cask.hydrator.plugin.transform.ValidatorTransform;
import com.google.common.collect.ObjectArrays;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

/**
 * Fields useful for unit tests, especially from other modules that want to use dataset-plugins in their tests.
 */
public class DatasetPluginClasses {
  public static final Class[] BATCH = {
    BatchCubeSink.class,
    KVTableSource.class, KVTableSink.class,
    SnapshotFileBatchAvroSource.class, SnapshotFileBatchAvroSink.class,
    AvroKeyOutputFormat.class, AvroKey.class, AvroKeyInputFormat.class,
    SnapshotFileBatchParquetSource.class, SnapshotFileBatchParquetSink.class,
    AvroParquetOutputFormat.class, AvroParquetInputFormat.class,
    S3AvroBatchSink.class, S3ParquetBatchSink.class,
    StreamBatchSource.class,
    TableSource.class, TableSink.class,
    TimePartitionedFileSetDatasetAvroSource.class, TimePartitionedFileSetDatasetAvroSink.class,
    TimePartitionedFileSetDatasetParquetSource.class, TimePartitionedFileSetDatasetParquetSink.class
  };
  public static final Class[] REALTIME = {
    DataGeneratorSource.class, JmsSource.class,
    TwitterSource.class, SqsSource.class,
    RealtimeCubeSink.class, RealtimeTableSink.class, StreamSink.class
  };
  public static final Class[] TRANSFORM = {
    ProjectionTransform.class, ScriptTransform.class, ScriptFilterTransform.class, ValidatorTransform.class,
    StructuredRecordToGenericRecordTransform.class
  };
  // for unit tests
  public static final Class[] ALL = ObjectArrays.concat(
    ObjectArrays.concat(BATCH, REALTIME, Class.class), TRANSFORM,
    Class.class
  );
}
