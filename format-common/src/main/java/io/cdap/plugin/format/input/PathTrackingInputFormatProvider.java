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

package io.cdap.plugin.format.input;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.MetadataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class for input format plugins that support tracking which file their record came from.
 *
 * @param <T> type of plugin config
 */
public abstract class PathTrackingInputFormatProvider<T extends PathTrackingConfig> implements ValidatingInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(PathTrackingInputFormatProvider.class);
  private static final String NAME_SCHEMA = "schema";
  protected T conf;

  protected PathTrackingInputFormatProvider(T conf) {
    this.conf = conf;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Map<String, String> properties = new HashMap<>();
    if (conf.getPathField() != null) {
      properties.put(PathTrackingInputFormat.PATH_FIELD, conf.getPathField());
      properties.put(PathTrackingInputFormat.FILENAME_ONLY, String.valueOf(conf.useFilenameOnly()));
    }
    if (conf.getLengthField() != null) {
      properties.put(MetadataField.FILE_LENGTH.getConfName(), conf.getLengthField());
    }
    if (conf.getModificationTimeField() != null) {
      properties.put(MetadataField.FILE_MODIFICATION_TIME.getConfName(), conf.getModificationTimeField());
    }
    if (conf.getSchema() != null) {
      properties.put(NAME_SCHEMA, conf.getSchema().toString());
    }

    addFormatProperties(properties);
    return properties;
  }

  /**
   * Perform validation on the provided configuration.
   *
   * Deprecated since 2.3.0. Use {@link PathTrackingInputFormatProvider#validate(FormatContext)} method instead.
   */
  @Deprecated
  protected void validate() {
    // no-op
  }

  public void validate(FormatContext context) {
    getSchema(context);
  }

  @Nullable
  @Override
  public Schema getSchema(FormatContext context) {
    FailureCollector collector = context.getFailureCollector();
    try {
      Schema formatSchema = conf.getSchema();
      if (formatSchema == null) {
        return null;
      }
      String lengthFieldResolved = null;
      String modificationTimeFieldResolved = null;

      // this is required for back compatibility with File-based sources (File, FTP...)
      try {
        lengthFieldResolved = conf.lengthField;
        modificationTimeFieldResolved = conf.modificationTimeField;
      } catch (NoSuchFieldError e) {
        LOG.warn("A modern PathTrackingConfig is used with old plugin.");
      }

      List<Schema.Field> fields = new ArrayList<>(formatSchema.getFields());

      extendSchemaWithMetadataField(fields, conf.pathField, Schema.Type.STRING);
      extendSchemaWithMetadataField(fields, lengthFieldResolved, Schema.Type.LONG);
      extendSchemaWithMetadataField(fields, modificationTimeFieldResolved, Schema.Type.LONG);

      return Schema.recordOf("record", fields);
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA).withStacktrace(e.getStackTrace());
    }
    throw collector.getOrThrowException();
  }

  private void extendSchemaWithMetadataField(List<Schema.Field> fields, String fieldName, Schema.Type type) {
    if (fieldName != null && !fieldName.isEmpty()) {
      boolean addField = true;

      Iterator<Schema.Field> i = fields.iterator();
      while (i.hasNext()) {
        Schema.Field field = i.next();
        if (field.getName().equals(fieldName)) {
          if (field.getSchema().equals(Schema.of(type))) {
            addField = false;
          } else {
            i.remove();
          }
        }
      }

      if (addField) {
        fields.add(Schema.Field.of(fieldName, Schema.of(type)));
      }
    }
  }

  /**
   * Add any format specific properties required by the InputFormat.
   *
   * @param properties properties to add to
   */
  protected void addFormatProperties(Map<String, String> properties) {
    // no-op
  }
}
