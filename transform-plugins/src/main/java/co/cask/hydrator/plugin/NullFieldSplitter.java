/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiOutputStageConfigurer;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Splits input data into two outputs based on whether a configurable field is null or not.
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("NullFieldSplitter")
@Description("This plugin is used when you want to split records based on whether a specific field is null or not. " +
  "Records with a null value for the field are sent to the 'null' port while records with a non-null " +
  "value are sent to the 'nonnull' port.")
public class NullFieldSplitter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  public static final String NULL_PORT = "null";
  public static final String NON_NULL_PORT = "nonnull";
  private final Conf conf;
  // only used when input schema is variable
  private Map<Schema, Schema> schemaMap;

  public NullFieldSplitter(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
    MultiOutputStageConfigurer stageConfigurer = multiOutputPipelineConfigurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema != null && !conf.containsMacro("field")) {
      stageConfigurer.setOutputSchemas(getOutputSchemas(inputSchema, conf));
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    schemaMap = new HashMap<>();
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null) {
      Schema nonNullSchema = getNonNullSchema(context.getInputSchema(), conf.field);
      schemaMap.put(inputSchema, nonNullSchema);
    }
  }

  @Override
  public void transform(StructuredRecord record, MultiOutputEmitter<StructuredRecord> emitter) {
    Schema recordSchema = record.getSchema();
    Object val = record.get(conf.field);
    if (val == null) {
      emitter.emit(NULL_PORT, record);
    } else if (!conf.modifySchema) {
      emitter.emit(NON_NULL_PORT, record);
    } else {
      Schema outputSchema = schemaMap.get(recordSchema);
      if (outputSchema == null) {
        outputSchema = getNonNullSchema(recordSchema, conf.field);
        schemaMap.put(recordSchema, outputSchema);
      }
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      for (Schema.Field recordField : record.getSchema().getFields()) {
        String fieldName = recordField.getName();
        builder.set(fieldName, record.get(fieldName));
      }
      emitter.emit(NON_NULL_PORT, builder.build());
    }
  }

  @Path("outputSchema")
  public Map<String, Schema> getOutputSchemas(GetSchemaRequest request) {
    return getOutputSchemas(request.inputSchema, request);
  }

  private static Map<String, Schema> getOutputSchemas(Schema inputSchema, Conf conf) {
    Map<String, Schema> outputs = new HashMap<>();
    if (inputSchema.getField(conf.field) == null) {
      throw new IllegalArgumentException("Field " + conf.field + " does not exist in input schema.");
    }

    outputs.put(NULL_PORT, inputSchema);
    outputs.put(NON_NULL_PORT, conf.modifySchema ? getNonNullSchema(inputSchema, conf.field) : inputSchema);

    return outputs;
  }

  @VisibleForTesting
  static Schema getNonNullSchema(Schema nullableSchema, String fieldName) {
    List<Schema.Field> fields = new ArrayList<>(nullableSchema.getFields().size());
    for (Schema.Field field : nullableSchema.getFields()) {
      Schema fieldSchema = field.getSchema();
      if (!field.getName().equals(fieldName) || fieldSchema.getType() != Schema.Type.UNION) {
        fields.add(field);
        continue;
      }

      List<Schema> unionSchemas = fieldSchema.getUnionSchemas();
      List<Schema> fieldSchemas = new ArrayList<>(unionSchemas.size());
      for (Schema unionSchema : unionSchemas) {
        if (unionSchema.getType() != Schema.Type.NULL) {
          fieldSchemas.add(unionSchema);
        }
      }

      if (fieldSchemas.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not contain a non-null type in its union schema.", fieldName));
      } else if (fieldSchemas.size() == 1) {
        fields.add(Schema.Field.of(fieldName, fieldSchemas.iterator().next()));
      } else {
        fields.add(Schema.Field.of(fieldName, Schema.unionOf(fieldSchemas)));
      }
    }
    return Schema.recordOf(nullableSchema.getRecordName() + ".nonnull", fields);
  }

  /**
   * Request to get output schemas
   */
  public static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
  }

  /**
   * Configuration for the plugin.
   */
  public static class Conf extends PluginConfig {
    @Macro
    @Description("Which field should be checked for null values.")
    private final String field;

    @Nullable
    @Description("Whether to modify the schema for non-null output. If set to true, the schema for non-null " +
      "output will be modified so that it is not nullable. Defaults to true.")
    private final Boolean modifySchema;

    private Conf() {
      this(null, true);
    }

    @VisibleForTesting
    public Conf(String field, Boolean modifySchema) {
      this.field = field;
      this.modifySchema = modifySchema;
    }
  }
}
