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
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToOrcTransformer;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write ORC records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("TPFSOrc")
@Description("Sink for a TimePartitionedFileSet that writes data in ORC format.")
@Requirements(datasetTypes = TimePartitionedFileSet.TYPE)
public class TimePartitionedFileSetDataSetORCSink extends TimePartitionedFileSetSink<NullWritable, OrcStruct> {
  private static final String ORC_COMPRESS = "orc.compress";
  private static final String SNAPPY_CODEC = "SNAPPY";
  private static final String ZLIB_CODEC = "ZLIB";
  private static final String COMPRESS_SIZE = "orc.compress.size";
  private static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  private static final String CREATE_INDEX = "orc.create.index";
  private final TPFSOrcSinkConfig config;
  private StructuredToOrcTransformer recordTransformer;

  public TimePartitionedFileSetDataSetORCSink(TPFSOrcSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToOrcTransformer();
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureORCFileSet(config.schema, properties);
    if (config.compressionCodec != null && !config.compressionCodec.equalsIgnoreCase("None")) {
      switch (config.compressionCodec.toUpperCase()) {
        case SNAPPY_CODEC:
          properties.setOutputProperty(ORC_COMPRESS, SNAPPY_CODEC);
          break;
        case ZLIB_CODEC:
          properties.setOutputProperty(ORC_COMPRESS, ZLIB_CODEC);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + config.compressionCodec);
      }
      if (config.compressionChunkSize != null) {
        properties.setOutputProperty(COMPRESS_SIZE, config.compressionChunkSize.toString());
      }
      if (config.stripeSize != null) {
        properties.setOutputProperty(COMPRESS_SIZE, config.stripeSize.toString());
      }
      if (config.indexStride != null) {
        properties.setOutputProperty(ROW_INDEX_STRIDE, config.indexStride.toString());
      }
      if (config.createIndex != null) {
        properties.setOutputProperty(CREATE_INDEX, config.indexStride.toString());
      }
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, OrcStruct>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), recordTransformer.transform(input, input.getSchema())));
  }

  /**
   * Config for TimePartitionedFileSetOrcSink
   */
  public static class TPFSOrcSinkConfig extends TPFSSinkConfig {

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    @Nullable
    @Description("Number of bytes in each compression chunk.")
    private Long compressionChunkSize;

    @Nullable
    @Description("Number of bytes in each stripe.")
    private Long stripeSize;

    @Nullable
    @Description("Number of rows between index entries (must be >= 1,000)")
    private Long indexStride;

    @Nullable
    @Description("Whether to create inline indexes")
    private Boolean createIndex;

    public TPFSOrcSinkConfig(String name, @Nullable String basePath, @Nullable String pathFormat,
                             @Nullable String timeZone, @Nullable String compressionCodec,
                             @Nullable Long compressionChunkSize, @Nullable Long stripeSize, @Nullable Long indexStride,
                             @Nullable String createIndex) {
      super(name, basePath, pathFormat, timeZone);
      this.compressionCodec = compressionCodec;
      this.compressionChunkSize = compressionChunkSize;
      this.stripeSize = stripeSize;
      this.indexStride = indexStride;
      this.createIndex = (createIndex != null && createIndex.equals("True"));
    }
  }
}
