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

package io.cdap.plugin.batch.source;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.plugin.common.Properties;
import io.cdap.plugin.common.RowRecordTransformer;
import io.cdap.plugin.common.TableSourceConfig;

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
