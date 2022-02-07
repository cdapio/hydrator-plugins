/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.delimited.common.DataTypeDetectorStatusKeeper;
import io.cdap.plugin.format.delimited.common.DataTypeDetectorUtils;
import io.cdap.plugin.format.input.PathTrackingConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common config for delimited related formats.
 */
public class DelimitedConfig extends PathTrackingConfig {

  // properties
  public static final String NAME_DELIMITER = "delimiter";
  public static final String NAME_ENABLE_QUOTES_VALUES = "enableQuotedValues";
  public static final String NAME_FORMAT = "format";
  public static final String NAME_OVERRIDE = "override";
  public static final String NAME_SAMPLE_SIZE = "sampleSize";
  public static final String NAME_PATH = "path";
  public static final String NAME_REGEX_PATH_FILTER = "fileRegex";
  public static final Map<String, PluginPropertyField> DELIMITED_FIELDS;

  // description
  public static final String DESC_ENABLE_QUOTES =
    "Whether to treat content between quotes as a value. The default value is false.";
  public static final String DESC_SKIP_HEADER =
    "Whether to skip the first line of each file. The default value is false.";

  static {
    Map<String, PluginPropertyField> fields = new HashMap<>(FIELDS);
    fields.put("skipHeader", new PluginPropertyField("skipHeader", DESC_SKIP_HEADER, "boolean", false, true));
    fields.put(NAME_ENABLE_QUOTES_VALUES,
      new PluginPropertyField(NAME_ENABLE_QUOTES_VALUES, DESC_ENABLE_QUOTES, "boolean", false, true));
    DELIMITED_FIELDS = Collections.unmodifiableMap(fields);
  }

  @Macro
  @Nullable
  @Description(DESC_SKIP_HEADER)
  private Boolean skipHeader;

  @Macro
  @Nullable
  @Description(DESC_ENABLE_QUOTES)
  protected Boolean enableQuotedValues;

  public boolean getSkipHeader() {
    return skipHeader == null ? false : skipHeader;
  }

  public boolean getEnableQuotedValues() {
    return enableQuotedValues == null ? false : enableQuotedValues;
  }

  public Long getSampleSize() {
    return Long.parseLong(getProperties().getProperties().getOrDefault(NAME_SAMPLE_SIZE, "1000"));
  }

  @Nullable
  @Override
  public Schema getSchema() {
    if (containsMacro(NAME_SCHEMA)) {
      return null;
    }
    if (Strings.isNullOrEmpty(schema)) {
      try {
        return getDefaultSchema(null);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }
    return super.getSchema();
  }

  /**
   * Reads delimiter from config. If not available returns default delimiter based on format.
   *
   * @return delimiter
   */
  private String getDefaultDelimiter() {
    String delimiter = getProperties().getProperties().get(NAME_DELIMITER);
    if (delimiter != null) {
      return delimiter;
    }
    final String format = getProperties().getProperties().get(NAME_FORMAT);
    switch (format) {
      case "tsv":
        return "\t";
      default:
        return ",";
    }
  }

  /**
   * Parses a list of key-value items of column names and their corresponding data types, manually set by the user.
   *
   * @return A hashmap of column names and their manually set schemas.
   */
  public HashMap<String, Schema> getOverride() throws IllegalArgumentException {
    String override = getProperties().getProperties().get(NAME_OVERRIDE);
    HashMap<String, Schema> overrideDataTypes = new HashMap<>();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(override)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(override)) {
        String name = keyVal.getKey();
        String stringDataType = keyVal.getValue();

        Schema schema = null;
        switch (stringDataType) {
          case "date":
            schema = Schema.of(Schema.LogicalType.DATE);
            break;
          case "time":
            schema = Schema.of(Schema.LogicalType.TIME_MICROS);
            break;
          case "timestamp":
            schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
            break;
          default:
            schema = Schema.of(Schema.Type.valueOf(stringDataType.toUpperCase()));
        }

        if (overrideDataTypes.containsKey(name)) {
          throw new IllegalArgumentException(String.format("Cannot convert '%s' to multiple types.", name));
        }
        overrideDataTypes.put(name, schema);
      }
    }
    return overrideDataTypes;
  }

  /**
   * Gets the detected schema.
   *
   * @param context {@link FormatContext}
   * @return The detected schema.
   * @throws IOException If the data can't be read from the datasource.
   */
  public Schema getDefaultSchema(@Nullable FormatContext context) throws IOException {
    final String format = getProperties().getProperties().getOrDefault(NAME_FORMAT, "delimited");
    String delimiter = getDefaultDelimiter();
    String regexPathFilter = getProperties().getProperties().get(NAME_REGEX_PATH_FILTER);
    String path = getProperties().getProperties().get(NAME_PATH);
    if (format.equals("delimited") && Strings.isNullOrEmpty(delimiter)) {
      throw new IllegalArgumentException("Delimiter is required when format is set to 'delimited'.");
    }

    Job job = JobUtils.createInstance();
    Configuration configuration = job.getConfiguration();
    for (Map.Entry<String, String> entry : getFileSystemProperties().entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    Path filePath = getFilePathForSchemaGeneration(path, regexPathFilter, configuration, job);
    DataTypeDetectorStatusKeeper dataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    String line = null;
    String[] columnNames = null;
    String[] rowValue = null;

    try (FileSystem fileSystem = JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                                                    f -> FileSystem.get(filePath.toUri(),
                                                                                        configuration));
         FSDataInputStream input = fileSystem.open(filePath);
         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
    ) {
      for (int rowIndex = 0; rowIndex < getSampleSize() && (line = bufferedReader.readLine()) != null; rowIndex++) {
        rowValue = line.split(delimiter, -1);
        if (rowIndex == 0) {
          columnNames = DataTypeDetectorUtils.setColumnNames(line, getSkipHeader(), delimiter);
          if (getSkipHeader()) {
            continue;
          }
        }
        DataTypeDetectorUtils.detectDataTypeOfRowValues(getOverride(), dataTypeDetectorStatusKeeper, columnNames,
                rowValue);
      }
      dataTypeDetectorStatusKeeper.validateDataTypeDetector();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to open file at path %s!", path), e);
    }
    List<Schema.Field> fields = DataTypeDetectorUtils.detectDataTypeOfEachDatasetColumn(getOverride(), columnNames,
            dataTypeDetectorStatusKeeper);
    return Schema.recordOf("text", fields);
  }
}
