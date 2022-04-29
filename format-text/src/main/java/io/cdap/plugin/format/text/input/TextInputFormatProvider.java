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

package io.cdap.plugin.format.text.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Input reading logic for text files.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(TextInputFormatProvider.NAME)
@Description(TextInputFormatProvider.DESC)
public class TextInputFormatProvider extends PathTrackingInputFormatProvider<TextInputFormatProvider.TextConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(PathTrackingInputFormatProvider.class);

  static final String NAME = "text";
  static final String DESC = "Plugin for reading files in text format.";
  public static final PluginClass PLUGIN_CLASS =
    new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, TextInputFormatProvider.class.getName(),
                    "conf", TextConfig.TEXT_FIELDS);
  // this has to be here to be able to instantiate the correct plugin property field
  private final TextConfig conf;

  public TextInputFormatProvider(TextConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public String getInputFormatClassName() {
    return CombineTextInputFormat.class.getName();
  }

  @Override
  protected void validate() {
    if (conf.containsMacro("schema")) {
      return;
    }

    List<String> fieldsToCheck = new ArrayList<>();
    fieldsToCheck.add(TextConfig.NAME_BODY);
    fieldsToCheck.add(TextConfig.NAME_OFFSET);
    fieldsToCheck.add(conf.getPathField());
    try {
      fieldsToCheck.add(conf.getLengthField());
      fieldsToCheck.add(conf.getModificationTimeField());
    } catch (NoSuchMethodError e) {
      LOG.warn("'Length' and 'Modification Time' properties are not supported by plugin.");
    }
    Schema schema = conf.getSchema();

    // text must contain 'body' as type 'string'.
    // it can optionally contain a 'offset' field of type 'long'
    // it can optionally contain a path field of type 'string'
    // it can optionally contain a length field of type 'long'
    // it can optionally contain a modificationTime field of type 'long'
    Schema.Field offsetField = schema.getField(TextConfig.NAME_OFFSET);
    if (offsetField != null) {
      Schema offsetSchema = offsetField.getSchema();
      Schema.Type offsetType = offsetSchema.isNullable() ? offsetSchema.getNonNullable().getType() :
        offsetSchema.getType();
      if (offsetType != Schema.Type.LONG) {
        throw new IllegalArgumentException(String.format("The 'offset' field must be of type 'long', but found '%s'",
                                                         offsetType.name().toLowerCase()));
      }
    }

    Schema.Field bodyField = schema.getField(TextConfig.NAME_BODY);
    if (bodyField == null) {
      throw new IllegalArgumentException(
        String.format("The schema for the 'text' format must have a field named '%s'", TextConfig.NAME_BODY));
    }
    Schema bodySchema = bodyField.getSchema();
    Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
    if (bodyType != Schema.Type.STRING) {
      throw new IllegalArgumentException(String.format("The '%s' field must be of type 'string', but found '%s'",
        TextConfig.NAME_BODY, bodyType.name().toLowerCase()));
    }

    // fields should be body (required), offset (optional), [pathfield, length, modificationTime] (optional)
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
        String.format("The schema for the 'text' format must only contain the %s field%s, but found %d other field%s",
                expectedFields, numExpectedFields > 1 ? "s" : "", numExtraFields,
                numExtraFields > 1 ? "s" : ""));
    }
  }

  @Override
  public void validate(FormatContext context) {
    if (conf.containsMacro(TextConfig.NAME_SCHEMA)) {
      return;
    }

    FailureCollector collector = context.getFailureCollector();
    Schema schema;
    try {
      schema = conf.getSchema();
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(TextConfig.NAME_SCHEMA)
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }

    List<String> fieldsToCheck = new ArrayList<>();
    fieldsToCheck.add(TextConfig.NAME_BODY);
    fieldsToCheck.add(TextConfig.NAME_OFFSET);
    fieldsToCheck.add(conf.getPathField());
    try {
      fieldsToCheck.add(conf.getLengthField());
      fieldsToCheck.add(conf.getModificationTimeField());
    } catch (NoSuchMethodError e) {
      LOG.warn("'Length' and 'Modification Time' properties are not supported by plugin.");
    }
    // text must contain 'body' as type 'string'.
    // it can optionally contain a 'offset' field of type 'long'
    // it can optionally contain a path field of type 'string'
    // it can optionally contain a length field of type 'long'
    // it can optionally contain a modificationTime field of type 'long'
    Schema.Field offsetField = schema.getField(TextConfig.NAME_OFFSET);
    if (offsetField != null) {
      Schema offsetSchema = offsetField.getSchema();
      offsetSchema = offsetSchema.isNullable() ? offsetSchema.getNonNullable() : offsetSchema;
      Schema.Type offsetType = offsetSchema.getType();
      if (offsetType != Schema.Type.LONG) {
        collector.addFailure(
          String.format("The 'offset' field is of unexpected type '%s'.", offsetSchema.getDisplayName()),
          "Change type to 'long'.").withOutputSchemaField(TextConfig.NAME_OFFSET);
      }
    }

    Schema.Field bodyField = schema.getField(TextConfig.NAME_BODY);
    if (bodyField == null) {
      collector.addFailure(
        String.format("The schema for the 'text' format must have a field named '%s'.", TextConfig.NAME_BODY), null)
        .withConfigProperty(TextConfig.NAME_SCHEMA);
    } else {
      Schema bodySchema = bodyField.getSchema();
      bodySchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
      Schema.Type bodyType = bodySchema.getType();
      if (bodyType != Schema.Type.STRING) {
        collector.addFailure(String.format("The '%s' field is of unexpected type '%s'.'",
            TextConfig.NAME_BODY, bodySchema.getDisplayName()),
          "Change type to 'string'.").withOutputSchemaField(TextConfig.NAME_BODY);
      }
    }

    // fields should be body (required), offset (optional), [pathfield] (optional)
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
          String.format("The schema for the 'text' format must only contain the '%s' field%s.",
                  expectedFields, expectedFields.length() > 1 ? "s" : ""),
          String.format("Remove additional field '%s'.", field.getName())).withOutputSchemaField(field.getName());
      }
    }
  }

  public static Schema getDefaultSchema(@Nullable String pathField, @Nullable String lengthField,
                                        @Nullable String modificationTimeField) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(TextConfig.NAME_OFFSET, Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of(TextConfig.NAME_BODY, Schema.of(Schema.Type.STRING)));
    if (pathField != null && !pathField.isEmpty()) {
      fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
    }
    if (lengthField != null && !lengthField.isEmpty()) {
      fields.add(Schema.Field.of(lengthField, Schema.of(Schema.Type.LONG)));
    }
    if (modificationTimeField != null && !modificationTimeField.isEmpty()) {
      fields.add(Schema.Field.of(modificationTimeField, Schema.of(Schema.Type.LONG)));
    }
    return Schema.recordOf("textfile", fields);
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    super.addFormatProperties(properties);
    properties.put(CombineTextInputFormat.SKIP_HEADER, String.valueOf(conf.getSkipHeader()));
  }

  /**
   * Text plugin config
   */
  public static class TextConfig extends PathTrackingConfig {
    public static final Map<String, PluginPropertyField> TEXT_FIELDS;
    private static final String NAME_SCHEMA = "schema";
    private static final String NAME_OFFSET = "offset";
    private static final String NAME_BODY = "body";

    private static final String SKIP_HEADER_DESC = "Whether to skip header for the files. " +
                                                     "Default value is false.";

    static {
      Map<String, PluginPropertyField> fields = new HashMap<>(FIELDS);
      fields.put("skipHeader", new PluginPropertyField("skipHeader", SKIP_HEADER_DESC,
                                                       "boolean", false, true));
      TEXT_FIELDS = Collections.unmodifiableMap(fields);
    }

    @Macro
    @Nullable
    @Description(SKIP_HEADER_DESC)
    protected Boolean skipHeader;

    public boolean getSkipHeader() {
      return skipHeader == null ? false : skipHeader;
    }

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
  }
}
