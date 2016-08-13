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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Parquet records to a {@link PartitionedFileSet}.
 */
@Plugin(type = "batchsink")
@Name("DynamicPFSParquet")
@Description("Sink for a PartitionedFileSet that writes data in Parquet format and uses a dynamic partition key.")
public class DynamicPartitionFileSetParquetSink extends
  PartitionedFileSetSink<Void, GenericRecord> {
  public static final String NAME = "DynamicPFSParquet";

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionFileSetParquetSink.class);
  private static final String SCHEMA_DESC = "The Parquet schema of the record being written to the Sink as a JSON " +
    "Object.";
  private static final String FIELD_DESC = "The fields to be used for the partitions.";

  private StructuredToAvroTransformer recordTransformer;
  private final DynamicPartitionParquetSinkConfig config;

  public DynamicPartitionFileSetParquetSink(DynamicPartitionParquetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = getAdditionalPFSArguments();
    PartitionedFileSetArguments.setDynamicPartitioner(sinkArgs, CustomDynamicPartitioner.class);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    String pfsName = config.name;
    String basePath = config.basePath == null ? pfsName : config.basePath;

    String schema = config.schema.toLowerCase();
    // parse to make sure it's valid
    new org.apache.avro.Schema.Parser().parse(schema);
    String hiveSchema;
    try {
      hiveSchema = HiveSchemaConverter.toHiveSchema(Schema.parseJson(schema));
    } catch (UnsupportedTypeException | IOException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }

    Partitioning.Builder partitionBuilder = Partitioning.builder();
    String[] partitionFields = config.fieldNames.split(",");
    for (int i = 0; i < partitionFields.length; i++) {
      partitionBuilder.addStringField("partition_" + partitionFields[i]);
    }

    pipelineConfigurer.createDataset(pfsName, PartitionedFileSet.class.getName(),
                                     PartitionedFileSetProperties.builder()
                                       .setPartitioning(partitionBuilder.build())
                                       .setBasePath(basePath)
                                       .setInputFormat(AvroParquetInputFormat.class)
                                       .setOutputFormat(AvroParquetOutputFormat.class)
                                       .setEnableExploreOnCreate(true)
                                       .setExploreFormat("parquet")
                                       .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
                                       .add(DatasetProperties.SCHEMA, schema)
                                       .build());
  }

  @Override
  protected Map<String, String> getAdditionalPFSArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "parquet.avro.schema", config.schema.toLowerCase());
    return args;
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

  /**
   * Config for DynamicPartitionFileSetParquetSink
   */
  public static class DynamicPartitionParquetSinkConfig extends PartitionedFileSetSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;
    @Description(FIELD_DESC)
    private String fieldNames;

    public DynamicPartitionParquetSinkConfig(String name, String schema, String fieldNames,
                                             @Nullable String basePath,
                                             @Nullable String pathFormat) {
      super(name, basePath, pathFormat);
      this.schema = schema;
      this.fieldNames = fieldNames;
    }
  }

  /**
   * Dynamic partitioner
   */
  public static final class CustomDynamicPartitioner extends DynamicPartitioner<Void, GenericRecord> {
    private String[] fieldNames;
    private String format;

    @Override
    public void initialize(MapReduceTaskContext mapReduceTaskContext) {
      fieldNames = mapReduceTaskContext
        .getPluginProperties(DynamicPartitionFileSetParquetSink.NAME)
        .getProperties().get("fieldNames").split(",");
      format = mapReduceTaskContext
        .getPluginProperties(DynamicPartitionFileSetParquetSink.NAME)
        .getProperties().get("partitionFormat");
    }

    @Override
    public PartitionKey getPartitionKey(Void key, GenericRecord value) {
      PartitionKey.Builder keyBuilder = PartitionKey.builder();
      for (int i = 0; i < fieldNames.length; i++) {
        String partitionValue = (format.equals("value")) ? String.valueOf(value.get(fieldNames[i]))
          : String.format("%s=%s", fieldNames[i],
                          value.get(fieldNames[i]).toString());
        keyBuilder.addStringField("partition_" + fieldNames[i], partitionValue);
      }
      return keyBuilder.build();
    }
  }

}
