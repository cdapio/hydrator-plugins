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

package io.cdap.plugin.batch.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.cdap.format.RecordPutTransformer;
import io.cdap.plugin.common.Properties;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.common.TableSinkConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CDAP Table Dataset Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("Table")
@Description("Writes records to a Table with one record field mapping to the Table rowkey," +
  " and all other record fields mapping to Table columns.")
@Requirements(datasetTypes = Table.TYPE)
public class TableSink extends BatchWritableSink<StructuredRecord, byte[], Put> {

  private final TableSinkConfig tableSinkConfig;
  private RecordPutTransformer recordPutTransformer;

  public TableSink(TableSinkConfig tableSinkConfig) {
    super(tableSinkConfig);
    this.tableSinkConfig = tableSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD) ||
                                  !Strings.isNullOrEmpty(tableSinkConfig.getRowField()),
                                "Row field must be given as a property.");
    Schema outputSchema =
      SchemaValidator.validateOutputSchemaAndInputSchemaIfPresent(tableSinkConfig.getSchemaStr(),
                                                                  tableSinkConfig.getRowField(), pipelineConfigurer);
    if (outputSchema != null) {
      if (outputSchema.getField(tableSinkConfig.getRowField()) == null) {
        throw new IllegalArgumentException("Output schema should contain the rowkey column.");
      }
      if (outputSchema.getFields().size() == 1) {
        throw new IllegalArgumentException("Output schema should have columns other than rowkey.");
      }
    }

    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  protected boolean shouldSkipCreateAtConfigure() {
    return tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA) ||
      tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    super.prepareRun(context);
    String schemaString = tableSinkConfig.getSchemaStr();
    if (schemaString != null) {
      try {
        Schema schema = Schema.parseJson(schemaString);
        if (schema.getFields() != null) {
          FieldOperation operation =
            new FieldWriteOperation("Write", "Wrote to CDAP Table",
                                    EndPoint.of(context.getNamespace(), tableSinkConfig.getName()),
                                    schema.getFields().stream().map(Schema.Field::getName)
                                      .collect(Collectors.toList()));
          context.record(Collections.singletonList(operation));
        }
      } catch (IOException e) {
        throw new IllegalStateException("Failed to parse schema.", e);
      }
    }

  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema outputSchema = null;
    // If a schema string is present in the properties, use that to construct the outputSchema and pass it to the
    // recordPutTransformer
    String schemaString = tableSinkConfig.getSchemaStr();
    if (schemaString != null) {
      outputSchema = Schema.parseJson(schemaString);
    }
    recordPutTransformer = new RecordPutTransformer(tableSinkConfig.getRowField(), outputSchema);
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties;
    properties = new HashMap<>(tableSinkConfig.getProperties().getProperties());

    properties.put(Properties.BatchReadableWritable.NAME, tableSinkConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    emitter.emit(new KeyValue<>(put.getRow(), put));
  }
}
