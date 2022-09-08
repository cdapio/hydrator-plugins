/*
 * Copyright © 2018-2022 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class for input format plugins that support tracking which file their record came from.
 *
 * @param <T> type of plugin config
 */
public abstract class PathTrackingInputFormatProvider<T extends PathTrackingConfig> implements ValidatingInputFormat {
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

  protected Schema addPathField(Schema dataSchema, FailureCollector failureCollector) {
    return addPathField(failureCollector, dataSchema, conf.getPathField());
  }

  public static Schema addPathField(FailureCollector failureCollector, Schema dataSchema, @Nullable String pathField) {
    if (pathField == null) {
      return dataSchema;
    }
    List<Schema.Field> fields = new ArrayList<>(dataSchema.getFields());
    for (Schema.Field field : fields) {
      if (pathField.equals(field.getName())) {
        failureCollector.addFailure(
          String.format("Path Field %s already exists in the data schema ", pathField),
            "Please provide a field that does not already exist")
                        .withConfigProperty("pathField");
        return dataSchema;
      }
    }
    fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(dataSchema.getRecordName(), fields);
  }

  public void validate(FormatContext context) {
    getSchema(context);
  }

  @Nullable
  @Override
  public Schema getSchema(FormatContext context) {
    FailureCollector collector = context.getFailureCollector();
    try {
      return conf.getSchema();
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA).withStacktrace(e.getStackTrace());
    }
    throw collector.getOrThrowException();
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
