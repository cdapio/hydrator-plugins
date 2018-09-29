/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.format.FileFormat;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * A default implementation of {@link FileSourceProperties}. Extend this class only if the plugin does not
 * need to change any of the property descriptions, names, macros, etc.
 */
@SuppressWarnings({"unused", "ConstantConditions"})
public abstract class AbstractFileSourceConfig extends PluginConfig implements FileSourceProperties {
  @Macro
  @Description("Name be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Macro
  @Nullable
  @Description("Regular expression that file paths must match in order to be included in the input. "
    + "The full file path is compared, not just the file name."
    + "If no value is given, no file filtering will be done. "
    + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
    + "the regular expression syntax.")
  private String fileRegex;

  @Macro
  @Nullable
  @Description("Format of the data to read. Supported formats are 'text', 'avro' or 'parquet'. "
    + "The default value is 'text'.")
  private String format;

  @Nullable
  @Description("Maximum size of each partition used to read data. "
    + "Smaller partitions will increase the level of parallelism, but will require more resources and overhead.")
  @Macro
  private Long maxSplitSize;

  @Nullable
  @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
    + "does not exist. When true, the run will not fail and the source will not generate any output. "
    + "The default value is false.")
  private Boolean ignoreNonExistingFolders;

  @Nullable
  @Description("Whether to recursively read directories within in the input directory. The default is false.")
  private Boolean recursive;

  @Nullable
  @Description("Output field to place the path of the file that the record was read from. "
    + "If not specified, the file path will not be included in output records. "
    + "If specified, the field must exist in the output schema as a string.")
  private String pathField;

  @Nullable
  @Description("Whether to only use the filename instead of the URI of the file path when a path field is given. "
    + "The default value is false.")
  private Boolean filenameOnly;

  @Nullable
  @Description("Output schema for the source. Formats like 'avro' and 'parquet' require a schema in order to "
    + "read the data.")
  private String schema;

  // this is a hidden property that only exists for wrangler's parse-as-csv that uses the header as the schema
  // when this is true and the format is text, the header will be the first record returned by every record reader
  @Nullable
  private Boolean copyHeader;

  protected AbstractFileSourceConfig() {
    // empty constructor is used to set default values.
    format = FileFormat.TEXT.name().toLowerCase();
    maxSplitSize = 128L * 1024 * 1024;
    ignoreNonExistingFolders = false;
    recursive = false;
    filenameOnly = false;
    copyHeader = false;
  }

  public void validate() {
    FileFormat fileFormat = null;
    if (!containsMacro("format")) {
      fileFormat = getFormat();
    }

    Schema schema = null;
    if (!containsMacro("schema")) {
      schema = getSchema();
    }

    if (fileFormat == FileFormat.TEXT && schema != null) {
      Schema.Field offsetField = schema.getField("offset");
      if (offsetField == null) {
        throw new IllegalArgumentException("Schema for text format must have a field named 'offset'");
      }
      Schema offsetSchema = offsetField.getSchema();
      Schema.Type offsetType = offsetSchema.isNullable() ? offsetSchema.getNonNullable().getType() :
        offsetSchema.getType();
      if (offsetType != Schema.Type.LONG) {
        throw new IllegalArgumentException("Type of 'offset' field must be 'long', but found " + offsetType);
      }

      Schema.Field bodyField = schema.getField("body");
      if (bodyField == null) {
        throw new IllegalArgumentException("Schema for text format must have a field named 'body'");
      }
      Schema bodySchema = bodyField.getSchema();
      Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
      if (bodyType != Schema.Type.STRING) {
        throw new IllegalArgumentException("Type of 'body' field must be 'string', but found + " + bodyType);
      }
    }

    if (pathField != null && schema != null) {
      Schema.Field schemaPathField = schema.getField(pathField);
      if (schemaPathField == null) {
        throw new IllegalArgumentException(
          String.format("Path field '%s' is not present in the schema. Please add it to the schema as a string field.",
                        pathField));
      }
      Schema pathFieldSchema = schemaPathField.getSchema();
      Schema.Type pathFieldType = pathFieldSchema.isNullable() ? pathFieldSchema.getNonNullable().getType() :
        pathFieldSchema.getType();
      if (pathFieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Path field '%s' must be of type 'string', but found '%s'.", pathField, pathFieldType));
      }
    }
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }

  @Override
  public FileFormat getFormat() {
    return FileFormat.from(format);
  }

  @Nullable
  @Override
  public Pattern getFilePattern() {
    return fileRegex == null ? null : Pattern.compile(fileRegex);
  }

  @Override
  public long getMaxSplitSize() {
    return maxSplitSize;
  }

  @Override
  public boolean shouldAllowEmptyInput() {
    return ignoreNonExistingFolders;
  }

  @Override
  public boolean shouldReadRecursively() {
    return recursive;
  }

  @Nullable
  @Override
  public String getPathField() {
    return pathField;
  }

  @Override
  public boolean useFilenameAsPath() {
    return filenameOnly;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
    }
  }

  public boolean shouldCopyHeader() {
    return copyHeader;
  }
}
