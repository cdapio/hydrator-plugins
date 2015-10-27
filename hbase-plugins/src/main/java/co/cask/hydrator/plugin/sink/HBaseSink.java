/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.RecordPutTransformer;
import co.cask.hydrator.plugin.HBaseConfig;
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

/**
 * Sink to write to HBase tables.
 */
@Plugin(type = "batchsink")
@Name("HBase")
@Description("HBase Batch Sink")
public class HBaseSink extends BatchSink<StructuredRecord, NullWritable, Mutation> {

  private HBaseSinkConfig config;
  private RecordPutTransformer recordPutTransformer;

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    context.addOutput(config.columnFamily, new HBaseOutputFormatProvider(config, conf));
    HBaseConfiguration.addHbaseResources(conf);
  }

  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      conf.put(TableOutputFormat.OUTPUT_TABLE, config.tableName);
      conf.put(TableOutputFormat.QUORUM_ADDRESS, String.format("%s:%s:%s", config.zkQuorum, config.zkClientPort,
                                                               config.zkNodeParent));
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
    Schema outputSchema = null;
    String schemaString = config.schema;
    if (schemaString != null) {
      outputSchema = Schema.parseJson(schemaString);
    }
    recordPutTransformer = new RecordPutTransformer(config.rowField, outputSchema);
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

  public static class HBaseSinkConfig extends HBaseConfig {
    private String zkNodeParent;
  }
}
