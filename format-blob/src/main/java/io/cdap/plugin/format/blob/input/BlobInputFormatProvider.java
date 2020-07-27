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

package io.cdap.plugin.format.blob.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the entire contents of a File into a single record
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(BlobInputFormatProvider.NAME)
@Description(BlobInputFormatProvider.DESC)
public class BlobInputFormatProvider extends PathTrackingInputFormatProvider<BlobInputFormatProvider.BlobConfig> {
  static final String NAME = "blob";
  static final String DESC = "Plugin for reading files in blob format.";
  public static final PluginClass PLUGIN_CLASS =
    new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, BlobInputFormatProvider.class.getName(),
                    "conf", PathTrackingConfig.FIELDS);

  public BlobInputFormatProvider(BlobConfig conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return PathTrackingBlobInputFormat.class.getName();
  }

  @Override
  public void validate() {
    if (conf.containsMacro("schema")) {
      return;
    }

    Schema schema = conf.getSchema();
    String pathField = conf.getPathField();
    Schema.Field bodyField = schema.getField("body");
    if (bodyField == null) {
      throw new IllegalArgumentException("The schema for the 'blob' format must have a field named 'body'");
    }
    Schema bodySchema = bodyField.getSchema();
    Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
    if (bodyType != Schema.Type.BYTES) {
      throw new IllegalArgumentException(String.format("The 'body' field must be of type 'bytes', but found '%s'",
                                                       bodyType.name().toLowerCase()));
    }

    // blob must contain 'body' as type 'bytes'.
    // it can optionally contain a path field of type 'string'
    int numExpectedFields = pathField == null ? 1 : 2;
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      int numExtra = numFields - numExpectedFields;
      if (pathField == null) {
        throw new IllegalArgumentException(
          String.format("The schema for the 'blob' format must only contain the 'body' field, "
                          + "but found %d other field%s.", numFields - 1, numExtra > 1 ? "s" : ""));
      } else {
        throw new IllegalArgumentException(
          String.format("The schema for the 'blob' format must only contain the 'body' field and the '%s' field, "
                          + "but found %d other field%s.", pathField, numFields - 2, numExtra > 1 ? "s" : ""));
      }
    }
  }

  @Override
  public void validate(FormatContext context) {
    if (conf.containsMacro(BlobConfig.NAME_SCHEMA)) {
      return;
    }

    FailureCollector collector = context.getFailureCollector();
    Schema schema;
    try {
      schema = conf.getSchema();
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(BlobConfig.NAME_SCHEMA)
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }

    String pathField = conf.getPathField();
    Schema.Field bodyField = schema.getField(BlobConfig.NAME_BODY);
    if (bodyField == null) {
      collector.addFailure("The schema for the 'blob' format must have a field named 'body' of type 'bytes'.", null)
        .withOutputSchemaField(BlobConfig.NAME_SCHEMA);
    } else {
      Schema bodySchema = bodyField.getSchema();
      Schema nonNullableSchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
      if (nonNullableSchema.getType() != Schema.Type.BYTES) {
        collector.addFailure(
          String.format("Field 'body' is of unexpected type '%s'.", nonNullableSchema.getDisplayName()),
          "Change type to 'bytes'.").withOutputSchemaField(BlobConfig.NAME_BODY);
      }
    }

    // blob must contain 'body' as type 'bytes'.
    // it can optionally contain a path field of type 'string'
    int numExpectedFields = pathField == null ? 1 : 2;
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      for (Schema.Field field : schema.getFields()) {
        if (pathField == null) {
          if (!field.getName().equals(BlobConfig.NAME_BODY)) {
            collector.addFailure("The schema for the 'blob' format must only contain the 'body' field.",
                                 String.format("Remove additional field '%s'.", field.getName()))
              .withOutputSchemaField(field.getName());
          }
        } else {
          if (!field.getName().equals(BlobConfig.NAME_BODY) && !field.getName().equals(pathField)) {
            collector.addFailure(
              String.format("The schema for the 'blob' format must only contain the 'body' field and '%s' field.",
                            pathField), String.format("Remove additional field '%s'.", field.getName()))
              .withOutputSchemaField(field.getName());
          }
        }
      }
    }
  }

  /**
   * Config for blob format. Overrides getSchema method to return the default schema if it is not provided.
   */
  public static class BlobConfig extends PathTrackingConfig {
    static final String NAME_SCHEMA = "schema";
    static final String NAME_BODY = "body";

    /**
     * Return the configured schema, or the default schema if none was given. Should never be called if the
     * schema contains a macro
     */
    @Override
    public Schema getSchema() {
      if (containsMacro(NAME_SCHEMA)) {
        return null;
      }
      if (Strings.isNullOrEmpty(schema)) {
        return getDefaultSchema();
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }

    private Schema getDefaultSchema() {
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(Schema.Field.of(NAME_BODY, Schema.of(Schema.Type.BYTES)));
      if (pathField != null && !pathField.isEmpty()) {
        fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
      }
      return Schema.recordOf("blob", fields);
    }
  }
}
