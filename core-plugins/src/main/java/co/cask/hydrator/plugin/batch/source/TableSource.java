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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.RowRecordTransformer;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.TableSourceConfig;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CDAP Table Dataset Batch Source.
 */
@Plugin(type = "batchsource")
@Name("Table")
@Requirements(datasetTypes = Table.TYPE)
@Description("Reads the entire contents of a CDAP Table. Outputs one record for each row in the Table.")
public class TableSource extends BatchReadableSource<byte[], Row, StructuredRecord> {
  private RowRecordTransformer rowRecordTransformer;

  private final TableSourceConfig tableConfig;

  public TableSource(TableSourceConfig tableConfig) {
    super(tableConfig);
    this.tableConfig = tableConfig;
  }

  @Override
  protected boolean shouldSkipCreateAtConfigure() {
    return tableConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA) ||
      tableConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD);
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(tableConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, tableConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema schema = tableConfig.getSchema();
    if (schema != null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws DatasetManagementException {
    super.prepareRun(context);
    Schema schema = tableConfig.getSchema();
    if (schema != null && schema.getFields() != null) {
      FieldOperation operation =
        new FieldReadOperation("Read", "Read from Table dataset",
                               EndPoint.of(context.getNamespace(), tableConfig.getName()),
                               schema.getFields().stream().map(Schema.Field::getName)
                                 .collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = tableConfig.getSchema();
    rowRecordTransformer = new RowRecordTransformer(schema, tableConfig.getRowField());
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(rowRecordTransformer.toRecord(input.getValue()));
  }
}
