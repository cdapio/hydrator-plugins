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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Reads the entire contents of a File into a single record
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(BlobInputFormatProvider.NAME)
@Description(BlobInputFormatProvider.DESC)
public class BlobInputFormatProvider extends PathTrackingInputFormatProvider<BlobInputFormatProvider.BlobConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(PathTrackingInputFormatProvider.class);

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

    List<String> fieldsToCheck = new ArrayList<>();
    fieldsToCheck.add(BlobConfig.NAME_BODY);
    fieldsToCheck.add(conf.getPathField());
    try {
      fieldsToCheck.add(conf.getLengthField());
      fieldsToCheck.add(conf.getModificationTimeField());
    } catch (NoSuchMethodError e) {
      LOG.warn("'Length' and 'Modification Time' properties are not supported by plugin.");
    }
    Schema.Field bodyField = schema.getField(BlobConfig.NAME_BODY);
    if (bodyField == null) {
      throw new IllegalArgumentException(
        String.format("The schema for the 'blob' format must have a field named '%s'", BlobConfig.NAME_BODY));
    }
    Schema bodySchema = bodyField.getSchema();
    Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
    if (bodyType != Schema.Type.BYTES) {
      throw new IllegalArgumentException(String.format("The '%s' field must be of type 'bytes', but found '%s'",
        BlobConfig.NAME_BODY, bodyType.name().toLowerCase()));
    }

    // blob must contain 'body' as type 'bytes'.
    // it can optionally contain a path field of type 'string'
    // it can optionally contain a length field of type 'long'
    // it can optionally contain a modificationTime field of type 'long'

    List<String> expectedFieldsList = fieldsToCheck
      .stream()
      .filter(Objects::nonNull)
      .collect(Collectors.toList());

    int numExpectedFields = expectedFieldsList.size();
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      String expectedFields = expectedFieldsList.stream().map(Object::toString)
              .collect(Collectors.joining("', '", "'", "'"));

      int numExtraFields = numFields - numExpectedFields;
      throw new IllegalArgumentException(
              String.format("The schema for the 'blob' format must only contain the %s field%s, " +
                              "but found %d other field%s",
                      expectedFields, numExpectedFields > 1 ? "s" : "", numExtraFields,
                      numExtraFields > 1 ? "s" : ""));
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

    List<String> fieldsToCheck = new ArrayList<>();
    fieldsToCheck.add(BlobConfig.NAME_BODY);
    fieldsToCheck.add(conf.getPathField());
    try {
      fieldsToCheck.add(conf.getLengthField());
      fieldsToCheck.add(conf.getModificationTimeField());
    } catch (NoSuchMethodError e) {
      LOG.warn("'Length' and 'Modification Time' properties are not supported by plugin.");
    }
    Schema.Field bodyField = schema.getField(BlobConfig.NAME_BODY);
    if (bodyField == null) {
      collector.addFailure(
        String.format("The schema for the 'blob' format must have a field named '%s' of type 'bytes'.",
          BlobConfig.NAME_BODY), null)
        .withOutputSchemaField(BlobConfig.NAME_SCHEMA);
    } else {
      Schema bodySchema = bodyField.getSchema();
      Schema nonNullableSchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
      if (nonNullableSchema.getType() != Schema.Type.BYTES) {
        collector.addFailure(String.format("Field '%s' is of unexpected type '%s'.",
            BlobConfig.NAME_BODY, nonNullableSchema.getDisplayName()),
          "Change type to 'bytes'.").withOutputSchemaField(BlobConfig.NAME_BODY);
      }
    }

    // blob must contain 'body' as type 'bytes'.
    // it can optionally contain a path field of type 'string'
    // it can optionally contain a length field of type 'long'
    List<String> expectedFieldsList = fieldsToCheck
      .stream()
      .filter(Objects::nonNull)
      .collect(Collectors.toList());

    int numExpectedFields = expectedFieldsList.size();
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      for (Schema.Field field : schema.getFields()) {
        String expectedFields = expectedFieldsList.stream().map(Object::toString)
                .collect(Collectors.joining(", ", "'", "'"));

        if (expectedFieldsList.contains(field.getName())) {
          continue;
        }

        collector.addFailure(
                String.format("The schema for the 'blob' format must only contain the '%s' field%s.",
                        expectedFields, expectedFields.length() > 1 ? "s" : ""),
                String.format("Remove additional field '%s'.", field.getName())).withOutputSchemaField(field.getName());
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
        String lengthFieldResolved = null;
        String modificationTimeFieldResolved = null;

        // this is required for back compatibility with File-based sources (File, FTP...)
        try {
          lengthFieldResolved = lengthField;
          modificationTimeFieldResolved = modificationTimeField;
        } catch (NoSuchFieldError e) {
          LOG.warn("'Length' and 'Modification Time' properties are not supported by plugin.");
        }
        return getDefaultSchema(pathField, lengthFieldResolved, modificationTimeFieldResolved);
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }

    private Schema getDefaultSchema(@Nullable String pathField, @Nullable String lengthField,
                                    @Nullable String modificationTimeField) {
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(Schema.Field.of(NAME_BODY, Schema.of(Schema.Type.BYTES)));
      if (pathField != null && !pathField.isEmpty()) {
        fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
      }
      if (lengthField != null && !lengthField.isEmpty()) {
        fields.add(Schema.Field.of(lengthField, Schema.of(Schema.Type.LONG)));
      }
      if (modificationTimeField != null && !modificationTimeField.isEmpty()) {
        fields.add(Schema.Field.of(modificationTimeField, Schema.of(Schema.Type.LONG)));
      }
      return Schema.recordOf("blob", fields);
    }
  }
}
