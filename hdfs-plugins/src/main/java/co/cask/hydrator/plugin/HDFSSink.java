/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HDFS Sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HDFS")
@Description("Batch HDFS Sink")
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
    // Verify if the timeSuffix format is valid.
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
    emitter.emit(new KeyValue<>(new Text(Joiner.on(",").join(dataArray)), NullWritable.get()));
  }

  /**
   * HDFS Sink Output Provider.
   */
  public static class SinkOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;
    private final HDFSSinkConfig config;

    public SinkOutputFormatProvider(HDFSSinkConfig config, BatchSinkContext context) {
      this.conf = new HashMap<>();
      this.config = config;
      String timeSuffix = !Strings.isNullOrEmpty(config.timeSufix) ?
        new SimpleDateFormat(config.timeSufix).format(context.getLogicalStartTime()) : "";
      conf.put(FileOutputFormat.OUTDIR, String.format("%s/%s", config.path, timeSuffix));
    }

    @Override
    public String getOutputFormatClassName() {
      return outputFormatClassName("TEXT");
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  private static String outputFormatClassName(String option) {
    // Use option to extend it to more output formats
    return TextOutputFormat.class.getName();
  }

  /**
   * Config for HDFSSinkConfig.
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

    public HDFSSinkConfig(String referenceName, String path, String suffix, String outputFormat) {
      super(referenceName);
      this.path = path;
      this.timeSufix = suffix;
    }

    private void validate() {
      // if macro provided, timeSuffix will be null at configure time
      if (!Strings.isNullOrEmpty(timeSufix)) {
        new SimpleDateFormat(timeSufix);
      }
    }
  }
}
