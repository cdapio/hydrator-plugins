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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Parquet records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = "batchsink")
@Name("TPFSParquet")
@Description("Sink for a TimePartitionedFileSet that writes data in Parquet format.")
public class TimePartitionedFileSetDatasetParquetSink extends
  TimePartitionedFileSetSink<Void, GenericRecord> {

  private static final String SCHEMA_DESC = "The Parquet schema of the record being written to the Sink as a JSON " +
    "Object.";
  private static final String SNAPPY_CODEC = "org.apache.hadoop.io.compress.SnappyCodec";
  private static final String GZIP_CODEC = "org.apache.hadoop.io.compress.GzipCodec";
  private static final String LZO_CODEC = "com.hadoop.compression.lzo.LzopCodec";
  private StructuredToAvroTransformer recordTransformer;
  private final TPFSParquetSinkConfig config;

  public TimePartitionedFileSetDatasetParquetSink(TPFSParquetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureParquetFileSet(config.schema, properties);
    if (config.compressionCodec != null && !config.compressionCodec.equals("None")) {
      properties.setOutputProperty("mapreduce.output.fileoutputformat.compress", "true");
      properties.setOutputProperty("mapreduce.output.fileoutputformat.compress.type", config.compressionType);
      switch (config.compressionCodec) {
        case "Snappy":
          properties.setOutputProperty("mapreduce.output.fileoutputformat.compress.codec", SNAPPY_CODEC);
          break;
        case "GZip":
          properties.setOutputProperty("mapreduce.output.fileoutputformat.compress.codec", GZIP_CODEC);
          break;
        case "LZO":
          properties.setOutputProperty("mapreduce.output.fileoutputformat.compress.codec", LZO_CODEC);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + config.compressionCodec);
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  @Override
  protected Map<String, String> getAdditionalTPFSArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "parquet.avro.schema", config.schema.toLowerCase());
    return args;
  }

  /**
   * Config for TimePartitionedFileSetParquetSink
   */
  public static class TPFSParquetSinkConfig extends TPFSSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;

    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    @Description("Used to specify the compression type to be used for the final dataset.")
    private String compressionType;

    public TPFSParquetSinkConfig(String name, String schema, @Nullable String basePath, @Nullable String pathFormat,
                                 @Nullable String timeZone, @Nullable String compressionCodec,
                                 @Nullable String compressionType) {
      super(name, basePath, pathFormat, timeZone);
      this.schema = schema;
      this.compressionCodec = compressionCodec;
      this.compressionType = compressionType;
    }
  }

}
