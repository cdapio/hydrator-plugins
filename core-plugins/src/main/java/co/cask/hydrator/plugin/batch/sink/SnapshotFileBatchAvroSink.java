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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.hydrator.format.StructuredToAvroTransformer;
import co.cask.hydrator.plugin.common.FileSetUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Avro format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotAvro")
@Description("Sink for a SnapshotFileSet that writes data in Avro format.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchAvroSink extends SnapshotFileBatchSink<AvroKey<GenericRecord>, NullWritable> {
  private StructuredToAvroTransformer recordTransformer;
  private final SnapshotAvroConfig config;

  public SnapshotFileBatchAvroSink(SnapshotAvroConfig config) {
    super(config);
    this.config = config;
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

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    FileSetUtil.configureAvroFileSet(config.schema, propertiesBuilder);
  }

  /**
   * Config for SnapshotFileBatchAvroSink
   */
  public static class SnapshotAvroConfig extends SnapshotFileSetBatchSinkConfig {
    @Description("The Avro schema of the record being written to the Sink as a JSON Object.")
    @Macro
    private String schema;

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    public SnapshotAvroConfig(String name, @Nullable String basePath, String schema,
                              @Nullable String compressionCodec) {
      super(name, basePath, null);
      this.schema = schema;
      this.compressionCodec = compressionCodec;
    }

    @Override
    public void validate() {
      super.validate();
      getSchema();
    }

    @Nullable
    public Schema getSchema() {
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }
  }
}
