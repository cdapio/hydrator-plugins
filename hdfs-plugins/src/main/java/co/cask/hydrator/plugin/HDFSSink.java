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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HDFS Sink
 */
//Removing this plugin from build as its deprecated and instead FileSink plugin been used.
//@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HDFS")
@Description("Batch HDFS Sink. (Deprecated. Use File Sink instead.)")
public class HDFSSink extends ReferenceBatchSink<StructuredRecord, Text, NullWritable> {
  public static final String NULL_STRING = "\0";
  private HDFSSinkConfig config;

  public HDFSSink(HDFSSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // if user provided macro, need to still validate timeSuffix format
    config.validate();
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(config, context)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, NullWritable>> emitter) throws Exception {
    List<String> dataArray = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      Object fieldValue = input.get(field.getName());
      String data = (fieldValue != null) ? fieldValue.toString() : NULL_STRING;
      dataArray.add(data);
    }
    emitter.emit(new KeyValue<>(new Text(Joiner.on(config.delimiter).join(dataArray)), NullWritable.get()));
  }

  /**
   * HDFS Sink Output Provider.
   */
  public static class SinkOutputFormatProvider implements OutputFormatProvider {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    private final Map<String, String> conf;

    public SinkOutputFormatProvider(HDFSSinkConfig config, BatchSinkContext context) {
      this.conf = new HashMap<>();
      String timeSuffix = !Strings.isNullOrEmpty(config.timeSufix) ?
        new SimpleDateFormat(config.timeSufix).format(context.getLogicalStartTime()) : "";
      conf.put(FileOutputFormat.OUTDIR, String.format("%s/%s", config.path, timeSuffix));
      if (!Strings.isNullOrEmpty(config.jobProperties)) {
        Map<String, String> arguments = GSON.fromJson(config.jobProperties, MAP_TYPE);
        for (Map.Entry<String, String> argument : arguments.entrySet()) {
          conf.put(argument.getKey(), argument.getValue());
        }
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return TextOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config for HDFS Sink.
   */
  public static class HDFSSinkConfig extends ReferencePluginConfig {

    @Name("path")
    @Description("HDFS Destination Path Prefix. For example, 'hdfs://mycluster.net:8020/output")
    @Macro
    private String path;

    @Name("suffix")
    @Description("Time Suffix used for destination directory for each run. For example, 'YYYY-MM-dd-HH-mm'. " +
      "By default, no time suffix is used.")
    @Nullable
    @Macro
    private String timeSufix;

    @Name("jobProperties")
    @Nullable
    @Description("Advanced feature to specify any additional properties that should be used with the sink, " +
      "specified as a JSON object of string to string. These properties are set on the job.")
    @Macro
    private String jobProperties;

    @Nullable
    @Description("The delimiter to use when concatenating record fields. Defaults to a comma (',').")
    @Macro
    private String delimiter;

    private HDFSSinkConfig() {
      super("");
      jobProperties = "{}";
      delimiter = ",";
    }

    private void validate() {
      // if macro provided, timeSuffix will be null at configure time
      if (!Strings.isNullOrEmpty(timeSufix)) {
        new SimpleDateFormat(timeSufix);
      }
      if (!Strings.isNullOrEmpty(jobProperties)) {
        // Try to parse the JSON and propagate the error
        new Gson().fromJson(jobProperties, new TypeToken<Map<String, String>>() { }.getType());
      }
    }
  }
}
