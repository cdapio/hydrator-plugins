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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.format.StructuredToAvroTransformer;
import co.cask.hydrator.plugin.common.FileSetUtil;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Parquet records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = "batchsink")
@Name("TPFSParquet")
@Description("Sink for a TimePartitionedFileSet that writes data in Parquet format.")
@Requirements(datasetTypes = TimePartitionedFileSet.TYPE)
public class TimePartitionedFileSetDatasetParquetSink extends TimePartitionedFileSetSink<Void, GenericRecord> {

  private StructuredToAvroTransformer recordTransformer;
  private final TPFSParquetSinkConfig config;

  public TimePartitionedFileSetDatasetParquetSink(TPFSParquetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureParquetFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getParquetCompressionConfiguration(config.compressionCodec, config.schema, true));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.getSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(null, recordTransformer.transform(input)));
  }

  /**
   * Config for TimePartitionedFileSetParquetSink
   */
  public static class TPFSParquetSinkConfig extends TPFSSinkConfig {

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    public TPFSParquetSinkConfig(String name, @Nullable String basePath, @Nullable String pathFormat,
                                 @Nullable String timeZone, @Nullable String compressionCodec) {
      super(name, basePath, pathFormat, timeZone);
      this.compressionCodec = compressionCodec;
    }
  }
}
