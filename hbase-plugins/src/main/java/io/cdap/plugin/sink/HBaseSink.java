/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.RecordPutTransformer;
import io.cdap.plugin.HBaseConfig;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.sink.mapreduce.HBaseTableOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink to write to HBase tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HBase")
@Description("HBase Batch Sink")
public class HBaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Mutation> {

  private HBaseSinkConfig config;
  private RecordPutTransformer recordPutTransformer;

  public HBaseSink(HBaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    Schema outputSchema =
      SchemaValidator.validateOutputSchemaAndInputSchemaIfPresent(config.schema, config.rowField, pipelineConfigurer);
    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    context.addOutput(Output.of(config.referenceName, new HBaseOutputFormatProvider(config, conf)));
  }

  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      conf.put(TableOutputFormat.OUTPUT_TABLE, config.tableName);
      String zkQuorum = !Strings.isNullOrEmpty(config.zkQuorum) ? config.zkQuorum : "localhost";
      String zkClientPort = !Strings.isNullOrEmpty(config.zkClientPort) ? config.zkClientPort : "2181";
      String zkNodeParent = !Strings.isNullOrEmpty(config.zkNodeParent) ? config.zkNodeParent : "/hbase";
      conf.put(TableOutputFormat.QUORUM_ADDRESS, String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));
      String[] serializationClasses = {
        configuration.get("io.serializations"),
        MutationSerialization.class.getName(),
        ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName() };
      conf.put("io.serializations", StringUtils.arrayToString(serializationClasses));
    }

    @Override
    public String getOutputFormatClassName() {
      return HBaseTableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // If a schema string is present in the properties, use that to construct the outputSchema and pass it to the
    // recordPutTransformer
    recordPutTransformer = new RecordPutTransformer(config.rowField, config.getSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    org.apache.hadoop.hbase.client.Put hbasePut = new org.apache.hadoop.hbase.client.Put(put.getRow());
    for (Map.Entry<byte[], byte[]> entry : put.getValues().entrySet()) {
      hbasePut.add(config.columnFamily.getBytes(), entry.getKey(), entry.getValue());
    }
    emitter.emit(new KeyValue<NullWritable, Mutation>(NullWritable.get(), hbasePut));
  }

  /**
   * HBaseSink plugin.
   */
  public static class HBaseSinkConfig extends HBaseConfig {
    @Description("Parent Node of HBase in Zookeeper. Defaults to '/hbase'")
    @Nullable
    private String zkNodeParent;

    public HBaseSinkConfig(String tableName, String rowField, @Nullable String schema) {
      super(String.format("HBase_%s", tableName), tableName, rowField, schema);
    }

    public HBaseSinkConfig(String referenceName, String tableName, String rowField, @Nullable String schema) {
      super(referenceName, tableName, rowField, schema);
    }

    public void validate(FailureCollector collector) {
      IdUtils.validateReferenceName(referenceName, collector);
      if (Strings.isNullOrEmpty(rowField)) {
        collector.addFailure("Row field must be given as a property.", null).withConfigProperty(NAME_ROWFIELD);
      }
    }
  }
}
