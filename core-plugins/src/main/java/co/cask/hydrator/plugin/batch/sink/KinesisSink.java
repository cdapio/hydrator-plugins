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
import co.cask.hydrator.plugin.common.KinesisOutputFormat;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("KinesisSink")
@Description("Sink that outputs to a specified AWS Kinesis stream.")
public class KinesisSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Text> {

  private static final String NULL_STRING = "\0";
  private static final Integer DEFAULT_SHARD_COUNT = 1;

  private final KinesisConfig config;

  public KinesisSink(KinesisConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.name),
                                "Stream name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessKey),
                                "Access Key should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessSecret),
                                "Access Key secret should be non-null, non-empty.");
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    batchSinkContext.addOutput(Output.of(config.referenceName, new KinesisOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    List<String> dataArray = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      Object fieldValue = input.get(field.getName());
      String data = (fieldValue != null) ? fieldValue.toString() : NULL_STRING;
      dataArray.add(data);
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(Joiner.on(",").join(dataArray))));
  }

  private static class KinesisOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;
    private KinesisOutputFormatProvider(KinesisConfig config) {
      this.conf = new HashMap<>();
      conf.put(Properties.KinesisRealtimeSink.ACCESS_ID, config.awsAccessKey);
      conf.put(Properties.KinesisRealtimeSink.ACCESS_KEY, config.awsAccessSecret);
      conf.put(Properties.KinesisRealtimeSink.SHARD_COUNT, String.valueOf(config.getShardCount()));
      conf.put(Properties.KinesisRealtimeSink.DISTRIBUTE, config.getDistribute());
      conf.put(Properties.KinesisRealtimeSink.NAME, config.name);
    }

    @Override
    public String getOutputFormatClassName() {
      return KinesisOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisConfig extends ReferencePluginConfig {

    @Description("The name of the Kinesis stream to output to. The stream will be created if it does not exist.")
    private String name;

    @Name(Properties.KinesisRealtimeSink.ACCESS_ID)
    @Description("AWS access Id having access to Kinesis streams")
    @Macro
    private String awsAccessKey;

    @Name(Properties.KinesisRealtimeSink.ACCESS_KEY)
    @Description("AWS access key secret having access to Kinesis streams")
    @Macro
    private String awsAccessSecret;

    @Name(Properties.KinesisRealtimeSink.SHARD_COUNT)
    @Description("Number of shards to be created, each shard has throughput of 1mb/s")
    @Macro
    @Nullable
    private Integer shardCount;

    @Name(Properties.KinesisRealtimeSink.DISTRIBUTE)
    @Description("Boolean to decide if the data has to be sent to a single shard or has to be uniformly distributed" +
      "among all the shards. Default value is true")
    private String distribute;

    public KinesisConfig(String referenceName, String name, String awsAccessKey,
                         String awsAccessSecret, Integer shardCount, String distribute) {
      super(referenceName);
      this.name = name;
      this.awsAccessKey = awsAccessKey;
      this.awsAccessSecret = awsAccessSecret;
      this.shardCount = shardCount;
      this.distribute = distribute;
    }

    public Integer getShardCount() {
      return shardCount == null ? DEFAULT_SHARD_COUNT : shardCount;
    }


    public String getDistribute() {
      //ensuring that distribute can have only 2 possible values "true" or "false". defaults to true
      return Boolean.toString(Boolean.parseBoolean(distribute));
    }

  }
}
