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

package co.cask.hydrator.plugin.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.plugin.HBaseConfig;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

/**
 *
 */
@Plugin(type = "batchsource")
@Name("HBase")
@Description("Read from an HBase table in batch")
public class HBaseSource extends ReferenceBatchSource<ImmutableBytesWritable, Result, StructuredRecord> {
  private RowRecordTransformer rowRecordTransformer;
  private HBaseConfig config;

  public HBaseSource(HBaseConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration conf = new Configuration();
    String ioSerializations = conf.get("io.serializations");
    conf.clear();

    conf.set(TableInputFormat.INPUT_TABLE, config.tableName);
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, config.columnFamily);
    String zkQuorum = !Strings.isNullOrEmpty(config.zkQuorum) ? config.zkQuorum : "localhost";
    String zkClientPort = !Strings.isNullOrEmpty(config.zkClientPort) ? config.zkClientPort : "2181";
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set("hbase.zookeeper.property.clientPort", zkClientPort);
    conf.setStrings(ioSerializations,
                    MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                    KeyValueSerialization.class.getName());
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(TableInputFormat.class, conf))
                       .alias(config.columnFamily));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = Schema.parseJson(config.schema);
    rowRecordTransformer = new RowRecordTransformer(schema, config.rowField);
  }

  @Override
  public void transform(KeyValue<ImmutableBytesWritable, Result> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    Row cdapRow = new co.cask.cdap.api.dataset.table.Result(
      input.getValue().getRow(), input.getValue().getFamilyMap(config.columnFamily.getBytes()));
    StructuredRecord record = rowRecordTransformer.toRecord(cdapRow);
    emitter.emit(record);
  }
}
