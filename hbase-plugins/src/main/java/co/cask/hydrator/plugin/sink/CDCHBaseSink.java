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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.HBaseConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.generic.GenericRecord;
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
public class CDCHBaseSink extends ReferenceBatchSink<GenericRecord, NullWritable, Mutation> {

  private HBaseSinkConfig config;
  //private StructuredToAvroTransformer avroTransformer;

  public CDCHBaseSink(HBaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).git
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);

    context.addOutput(Output.of(config.referenceName, new HBaseOutputFormatProvider(config, conf)));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.rowField),
                                "Row field must be given as a property.");
    Schema outputSchema =
      SchemaValidator.validateOutputSchemaAndInputSchemaIfPresent(config.schema,
                                                                  config.rowField, pipelineConfigurer);
    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
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
      return TableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(GenericRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    String table_name = (String) input.get("table_name");
    int schema_hash = (int) input.get("schema_hash");
    byte[] payload = (byte[]) input.get("payload");
    org.apache.avro.Schema schema = input.getSchema();
    Preconditions.checkArgument(schema.getType().equals(org.apache.avro.Schema.Type.RECORD),
                                "input is not an Avro record");


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
  }
}
