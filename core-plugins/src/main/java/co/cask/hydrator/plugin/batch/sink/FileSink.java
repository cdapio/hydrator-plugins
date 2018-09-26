/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Writes to the FileSystem.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("File")
@Description("Writes to the FileSystem.")
public class FileSink extends ReferenceBatchSink<StructuredRecord, Object, Object> {
  private static final String NULL_STRING = "\0";
  private Conf config;
  private Function<StructuredRecord, KeyValue<Object, Object>> recordTransformer;

  public FileSink(Conf config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException {
    // if user provided macro, need to still validate
    config.validate(context.getInputSchema());
    Job job = JobUtils.createInstance();
    Configuration hConf = job.getConfiguration();
    hConf.set(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime()));
    for (Map.Entry<String, String> entry : config.getFSProperties().entrySet()) {
      hConf.set(entry.getKey(), entry.getValue());
    }

    Schema schema = config.getSchema(context.getInputSchema());

    // create the external dataset with the given schema
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);


    if ("text".equals(config.format)) {
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(TextOutputFormat.class, hConf)));
    } else if ("avro".equals(config.format)) {
      if (schema == null) {
        throw new IllegalStateException("Cannot write using avro format when schema is unknown. "
                                          + "Please set the schema explicitly.");
      }
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
      AvroJob.setOutputKeySchema(job, avroSchema);
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(AvroKeyOutputFormat.class, hConf)));
    } else if ("parquet".equals(config.format)) {
      if (schema == null) {
        throw new IllegalStateException("Cannot write using parquet format when schema is unknown. "
                                          + "Please set the schema explicitly.");
      }
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
      AvroParquetOutputFormat.setSchema(job, avroSchema);
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(AvroParquetOutputFormat.class, hConf)));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = config.getSchema(context.getInputSchema());
    String schemaStr = schema == null ? null : schema.toString();

    if ("avro".equalsIgnoreCase(config.format)) {
      StructuredToAvroTransformer avroTransformer = new StructuredToAvroTransformer(schemaStr);
      recordTransformer = input -> {
        try {
          GenericRecord record = avroTransformer.transform(input);
          return new KeyValue<>(new AvroKey<>(record), NullWritable.get());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    } else if ("parquet".equalsIgnoreCase(config.format)) {
      StructuredToAvroTransformer avroTransformer = new StructuredToAvroTransformer(schemaStr);
      recordTransformer = input -> {
        try {
          GenericRecord record = avroTransformer.transform(input);
          return new KeyValue<>(null, record);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    } else if ("text".equalsIgnoreCase(config.format)) {
      recordTransformer = input -> {
        List<String> dataArray = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
          Object fieldValue = input.get(field.getName());
          String data = (fieldValue != null) ? fieldValue.toString() : NULL_STRING;
          dataArray.add(data);
        }
        return new KeyValue<>(new Text(Joiner.on(config.delimiter).join(dataArray)), NullWritable.get());
      };
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter) throws Exception {
    emitter.emit(recordTransformer.apply(input));
  }

  /**
   * Config for File Sink.
   */
  public static class Conf extends ReferencePluginConfig {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
    private static final String TEXT = "text";
    private static final String AVRO = "avro";
    private static final String PARQUET = "parquet";

    @Macro
    @Description("Destination path prefix. For example, 'hdfs://mycluster.net:8020/output'")
    private String path;

    @Macro
    @Description("Time suffix used for the destination directory for each run. For example, 'YYYY-MM-dd-HH-mm'. " +
      "By default, no time suffix is used.")
    @Nullable
    private String suffix;

    @Macro
    @Nullable
    @Description("Advanced feature to specify any additional properties that should be used with the sink.")
    private String fileSystemProperties;

    @Macro
    @Nullable
    @Description("The delimiter to use when concatenating record fields and the format is 'text'. "
      + "Defaults to a comma (',').")
    private String delimiter;

    @Macro
    @Nullable
    @Description("The format to write in. Must be 'text', 'avro', or 'parquet'. Defaults to 'text'.")
    public String format;

    @Macro
    @Nullable
    @Description("The schema to use when the format is 'avro' or 'parquet'. If none is specified, the input schema "
      + "will be used.")
    public String schema;

    private Conf() {
      super("");
      fileSystemProperties = "{}";
      delimiter = ",";
      format = "text";
    }

    private String getOutputDir(long logicalStartTime) {
      String timeSuffix = !Strings.isNullOrEmpty(suffix) ? new SimpleDateFormat(suffix).format(logicalStartTime) : "";
      return String.format("%s/%s", path, timeSuffix);
    }

    @Nullable
    private Schema getSchema(@Nullable Schema inputSchema) {
      if (containsMacro("schema")) {
        return null;
      }
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
      }
    }

    private void validate(@Nullable Schema inputSchema) {
      // if macro provided, timeSuffix will be null at configure time
      if (!Strings.isNullOrEmpty(suffix)) {
        new SimpleDateFormat(suffix);
      }
      if (!Strings.isNullOrEmpty(fileSystemProperties)) {
        // Try to parse the JSON and propagate the error
        new Gson().fromJson(fileSystemProperties, new TypeToken<Map<String, String>>() { }.getType());
      }
      if (!isText() && !isAvro() && !isParquet()) {
        throw new IllegalArgumentException(String.format("Invalid format '%s. Must be 'text', 'avro', or 'parquet'.",
                                                         format));
      }
      getFSProperties();
      if (!containsMacro("schema")) {
        Schema schema = getSchema(inputSchema);
        if ((isAvro() || isParquet()) && schema == null) {
          throw new IllegalArgumentException(
            String.format("Format '%s' cannot be used without a schema. Please set the schema property.", format));
        }
      }
    }

    private boolean isAvro() {
      return AVRO.equalsIgnoreCase(format);
    }

    private boolean isParquet() {
      return PARQUET.equalsIgnoreCase(format);
    }

    private boolean isText() {
      return TEXT.equalsIgnoreCase(format);
    }

    private Map<String, String> getFSProperties() {
      if (fileSystemProperties == null || fileSystemProperties.isEmpty()) {
        return Collections.emptyMap();
      }
      try {
        return GSON.fromJson(fileSystemProperties, MAP_TYPE);
      } catch (JsonSyntaxException e) {
        throw new IllegalArgumentException("Unable to parse filesystem properties: " + e.getMessage(), e);
      }
    }
  }
}
