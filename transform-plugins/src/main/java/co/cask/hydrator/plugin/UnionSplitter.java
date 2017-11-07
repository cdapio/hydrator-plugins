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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiOutputStageConfigurer;
import co.cask.cdap.etl.api.SplitterTransform;
import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Splits input between multiple output ports, with one port per possible type in the union.
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("UnionSplitter")
@Description("Splits input between multiple output ports, with one port per possible type in a field's union schema. " +
  "Enums, maps, and arrays inside the union are not supported. If the value is a record, the record schema name will " +
  "be used as the port. If the value is a simple type, the schema type will be used as the port (null, bytes, " +
  "bool, int, long, float, double, or string).")
public class UnionSplitter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private final Conf conf;

  public UnionSplitter(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
    MultiOutputStageConfigurer stageConfigurer = multiOutputPipelineConfigurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema == null) {
      return;
    }

    stageConfigurer.setOutputSchemas(getOutputSchemas(inputSchema, conf.unionField, conf.modifySchema));
  }

  @Override
  public void transform(StructuredRecord record, MultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    if (conf.unionField == null) {
      emitter.emit(record.getSchema().getRecordName(), record);
      return;
    }

    Schema.Field schemaField = record.getSchema().getField(conf.unionField);
    if (schemaField == null) {
      emitter.emitError(new InvalidEntry<>(100, String.format("Field '%s' does not exist.", conf.unionField), record));
      return;
    }

    Schema fieldSchema = schemaField.getSchema();
    if (fieldSchema.getType() != Schema.Type.UNION) {
      emitter.emitError(new InvalidEntry<>(200, String.format("Field '%s' is not of type union, but is of type '%s'.",
                                                              conf.unionField, fieldSchema.getType()), record));
      return;
    }

    Object val = record.get(conf.unionField);
    Schema valSchema;
    if (val == null) {
      valSchema = Schema.of(Schema.Type.NULL);
    } else if (val instanceof Boolean) {
      valSchema = Schema.of(Schema.Type.BOOLEAN);
    } else if (val instanceof ByteBuffer || val instanceof byte[] || val instanceof Byte[]) {
      valSchema = Schema.of(Schema.Type.BYTES);
    } else if (val instanceof Integer) {
      valSchema = Schema.of(Schema.Type.INT);
    } else if (val instanceof Long) {
      valSchema = Schema.of(Schema.Type.LONG);
    } else if (val instanceof Float) {
      valSchema = Schema.of(Schema.Type.FLOAT);
    } else if (val instanceof Double) {
      valSchema = Schema.of(Schema.Type.DOUBLE);
    } else if (val instanceof String) {
      valSchema = Schema.of(Schema.Type.STRING);
    } else if (val instanceof StructuredRecord) {
      valSchema = ((StructuredRecord) val).getSchema();
    } else if (val.getClass().isEnum()) {
      emitter.emitError(
        new InvalidEntry<>(300, String.format("Field '%s' is an Enum, which is not supported.", conf.unionField),
                           record));
      return;
    } else if (val instanceof Map) {
      emitter.emitError(
        new InvalidEntry<>(301, String.format("Field '%s' is a Map, which is not supported.", conf.unionField),
                           record));
      return;
    } else if (val instanceof Collection) {
      emitter.emitError(
        new InvalidEntry<>(302, String.format("Field '%s' is an array, which is not supported.", conf.unionField),
                           record));
      return;
    } else {
      emitter.emitError(
        new InvalidEntry<>(303, String.format("Could not determine type for field '%s' with value of class '%s'.",
                                              conf.unionField, val.getClass().getName()),
                           record));
      return;
    }

    boolean foundSchema = false;
    for (Schema unionSchema : fieldSchema.getUnionSchemas()) {
      // if the schema in the union matches the value's schema
      if (unionSchema.equals(valSchema)) {
        foundSchema = true;
      }
    }
    if (!foundSchema) {
      emitter.emitError(
        new InvalidEntry<>(400, String.format("Field '%s' has schema '%s', which is not in its union schema.",
                                              conf.unionField, valSchema), record));
      return;
    }

    Schema inputSchema = record.getSchema();
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields().size());
    for (Schema.Field inputSchemaField : inputSchema.getFields()) {
      String fieldName = inputSchemaField.getName();
      if (fieldName.equals(conf.unionField)) {
        fields.add(Schema.Field.of(fieldName, valSchema));
      } else {
        fields.add(inputSchemaField);
      }
    }

    Schema.Type valType = valSchema.getType();
    String port = valType == Schema.Type.RECORD ? valSchema.getRecordName() : valType.name().toLowerCase();
    Schema outputSchema = conf.modifySchema ?
      Schema.recordOf(inputSchema.getRecordName() + "." + port, fields) : inputSchema;
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputSchemaField : inputSchema.getFields()) {
      String fieldName = inputSchemaField.getName();
      builder.set(fieldName, record.get(fieldName));
    }
    emitter.emit(port, builder.build());
  }

  @Path("outputSchema")
  public Map<String, Schema> getOutputSchemas(GetSchemaRequest request) {
    return getOutputSchemas(request.inputSchema, request.unionField, request.modifySchema);
  }

  @VisibleForTesting
  static Map<String, Schema> getOutputSchemas(Schema inputSchema, String unionField, boolean modifySchema) {
    Map<String, Schema> outputPortSchemas = new HashMap<>();
    if (unionField == null) {
      outputPortSchemas.put(inputSchema.getRecordName(), inputSchema);
      return outputPortSchemas;
    }

    Schema.Field unionSchemaField = inputSchema.getField(unionField);
    if (unionSchemaField == null) {
      throw new IllegalArgumentException(
        String.format("Field '%s' does not exist in the input schema.", unionField));
    }
    Schema unionSchema = unionSchemaField.getSchema();
    if (unionSchema.getType() != Schema.Type.UNION) {
      throw new IllegalArgumentException(
        String.format("Field '%s' is not of type union, but is of type '%s'", unionField, unionSchema.getType()));
    }

    int numFields = inputSchema.getFields().size();
    ArrayList<Schema.Field> outputFields = new ArrayList<>(numFields);
    int i = 0;
    int unionIndex = -1;
    for (Schema.Field inputField : inputSchema.getFields()) {
      if (inputField.getName().equals(unionField)) {
        unionIndex = i;
        outputFields.add(null);
      } else {
        outputFields.add(inputField);
      }
      i++;
    }

    for (Schema schema : unionSchema.getUnionSchemas()) {
      Schema.Type type = schema.getType();
      switch (type) {
        case ENUM:
        case MAP:
        case ARRAY:
        case UNION:
          throw new IllegalArgumentException(String.format("A type of '%s' within a union is not supported.", type));
      }

      String port = type == Schema.Type.RECORD ? schema.getRecordName() : type.name().toLowerCase();
      outputFields.set(unionIndex, Schema.Field.of(unionField, modifySchema ? schema : unionSchema));
      outputPortSchemas.put(port, Schema.recordOf(inputSchema.getRecordName() + "." + port, outputFields));
    }

    return outputPortSchemas;
  }

  /**
   * Request to get output schemas.
   */
  static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
  }

  /**
   * Plugin conf
   */
  public static class Conf extends PluginConfig {
    @Description("The union field to split on. Each possible schema in the union will be emitted to a different " +
      "port. Only unions of records and simple types are supported. In other words, " +
      "enums, maps, and arrays in the union are not supported.")
    protected String unionField;

    @Nullable
    @Description("Whether to modify the schema of records before emitting them. If true, " +
      "the schema of the union field will be modified to be only a single schema matching the value of the field. " +
      "Defaults to true.")
    protected Boolean modifySchema;

    private Conf() {
      this(null, true);
    }

    @VisibleForTesting
    public Conf(String unionField, Boolean modifySchema) {
      this.unionField = unionField;
      this.modifySchema = modifySchema;
    }
  }
}
