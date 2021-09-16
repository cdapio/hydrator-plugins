/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.avro;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.RecordConverter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Create StructuredRecords from GenericRecords
 */
public class AvroToStructuredTransformer extends RecordConverter<GenericRecord, StructuredRecord> {

  private static final Gson GSON = new Gson();

  private final Map<Integer, Schema> schemaCache = Maps.newHashMap();
  private final Map<String, JsonObject> customTypes = Maps.newHashMap();

  public StructuredRecord transform(GenericRecord genericRecord) throws IOException {
    org.apache.avro.Schema genericRecordSchema = genericRecord.getSchema();
    return transform(genericRecord, convertSchema(genericRecordSchema));
  }

  @Override
  public StructuredRecord transform(GenericRecord genericRecord, Schema structuredSchema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : structuredSchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, convertField(genericRecord.get(fieldName), field));
    }
    return builder.build();
  }

  public StructuredRecord.Builder transform(GenericRecord genericRecord, Schema structuredSchema,
                                            @Nullable String skipField) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : structuredSchema.getFields()) {
      String fieldName = field.getName();
      if (!fieldName.equals(skipField)) {
        builder.set(fieldName, convertField(genericRecord.get(fieldName), field));
      }
    }

    return builder;
  }

  /**
   * Overriding here to validate for datetime field
   *
   * @param field  Field value
   * @param schema {@link Schema}
   * @return Object value
   * @throws IOException
   */
  protected Object convertField(Object field, Schema schema) throws IOException {
    if (schema.getLogicalType() == Schema.LogicalType.DATETIME) {
      try {
        LocalDateTime.parse(field.toString());
      } catch (DateTimeParseException exception) {
        throw new UnexpectedFormatException(
          String.format("Datetime value '%s' is not in ISO-8601 format.", field.toString()), exception);
      }
    }
    return super.convertField(field, schema);
  }

  public Schema convertSchema(org.apache.avro.Schema schema) throws IOException {
    int hashCode = schema.hashCode();
    Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      String strSchema = schema.toString();
      String jsonSchema = preprocessSchema(GSON.fromJson(strSchema, JsonObject.class)).toString();
      structuredSchema = Schema.parseJson(jsonSchema);
      schemaCache.put(hashCode, structuredSchema);
    }
    return structuredSchema;
  }

  private JsonObject preprocessSchema(JsonObject schema) {

    if (!schema.has("type")) {
      return schema;
    }

    JsonElement type = schema.get("type");

    if (type.isJsonArray()) {   // Union
      JsonArray processedUnion = preprocessUnion(type.getAsJsonArray());
      schema.remove("type");
      schema.add("type", processedUnion);
    } else if (type.isJsonObject()) {   // Unnamed Complex type

      preprocessSchema(type.getAsJsonObject());
    } else {
      String typeName = type.getAsString();

      switch (typeName) {
        case "record":
          for (JsonElement field : schema.get("fields").getAsJsonArray()) {
            preprocessSchema(field.getAsJsonObject());
          }
          customTypes.put(schema.getAsJsonPrimitive("name").getAsString(), schema);
          break;

        case "map":
          schema.addProperty("keys", "string");
          JsonElement value = schema.get("values");
          processElementInSchema(schema, value, "values");
          break;

        case "array":
          JsonElement items = schema.get("items");
          processElementInSchema(schema, items, "items");
          break;

        case "enum":
          customTypes.put(schema.getAsJsonPrimitive("name").getAsString(), schema);
          break;

        default:
          if (customTypes.containsKey(typeName)) {
            schema.remove("type");
            schema.add("type", customTypes.get(typeName));
          }
          break;
      }
    }

    return schema;
  }

  private JsonArray preprocessUnion(JsonArray schema) {
    JsonArray processedSchema = new JsonArray();

    for (JsonElement subschema: schema) {
      if (subschema.isJsonPrimitive()) {
        if (customTypes.containsKey(subschema.getAsString())) {
          processedSchema.add(customTypes.get(subschema.getAsString()));
        } else {
          processedSchema.add(subschema);
        }
      } else {
        preprocessSchema(subschema.getAsJsonObject());
        processedSchema.add(subschema);
      }
    }

    return processedSchema;
  }

  private void processElementInSchema(JsonObject schema, JsonElement element, String property) {
    if (element.isJsonObject()) {
      preprocessSchema(element.getAsJsonObject());
    } else if (element.isJsonArray()) {
      JsonArray processedItems = preprocessUnion(element.getAsJsonArray());
      schema.remove(property);
      schema.add(property, processedItems);
    } else if (customTypes.containsKey(element.getAsString())) {
      schema.remove(property);
      schema.add(property, customTypes.get(element.getAsString()));
    }
  }
}
