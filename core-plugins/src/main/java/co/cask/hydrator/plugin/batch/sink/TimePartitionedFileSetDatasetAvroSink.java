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
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("TPFSAvro")
@Description("Sink for a TimePartitionedFileSet that writes data in Avro format.")
@Requirements(datasetTypes = TimePartitionedFileSet.TYPE)
public class TimePartitionedFileSetDatasetAvroSink extends
  TimePartitionedFileSetSink<AvroKey<GenericRecord>, NullWritable> {
  private StructuredToAvroTransformer recordTransformer;
  private final TPFSAvroSinkConfig config;

  public TimePartitionedFileSetDatasetAvroSink(TPFSAvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureAvroFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getAvroCompressionConfiguration(config.compressionCodec, config.schema, true));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.getSchema());
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Config for TimePartitionedFileSetAvroSink
   */
  public static class TPFSAvroSinkConfig extends TPFSSinkConfig {

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    public TPFSAvroSinkConfig(String name, @Nullable String basePath, @Nullable String pathFormat,
                              @Nullable String timeZone, @Nullable String compressionCodec) {
      super(name, basePath, pathFormat, timeZone);
      this.compressionCodec = compressionCodec;
    }
  }
}
