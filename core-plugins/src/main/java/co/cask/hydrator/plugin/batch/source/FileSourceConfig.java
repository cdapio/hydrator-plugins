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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract file source config
 */
public abstract class FileSourceConfig extends ReferencePluginConfig {
  protected static final String MAX_SPLIT_SIZE_DESCRIPTION = "Maximum split-size for each mapper in the MapReduce " +
    "Job. Defaults to 128MB.";
  protected static final String TABLE_DESCRIPTION = "Name of the Table that keeps track of the last time files " +
    "were read in. If this is null or empty, the Regex is used to filter filenames.";
  protected static final String INPUT_FORMAT_CLASS_DESCRIPTION = "Name of the input format class, which must be a " +
    "subclass of FileInputFormat. Defaults to a CombinePathTrackingInputFormat, which is a customized version of " +
    "CombineTextInputFormat that records the file path each record was read from.";
  protected static final String REGEX_DESCRIPTION = "Regex to filter out files in the path. It accepts regular " +
    "expression which is applied to the complete path and returns the list of files that match the specified pattern." +
    "To use the TimeFilter, input \"timefilter\". The TimeFilter assumes that it " +
    "is reading in files with the File log naming convention of 'YYYY-MM-DD-HH-mm-SS-Tag'. The TimeFilter " +
    "reads in files from the previous hour if the field 'timeTable' is left blank. If it's currently " +
    "2015-06-16-15 (June 16th 2015, 3pm), it will read in files that contain '2015-06-16-14' in the filename. " +
    "If the field 'timeTable' is present, then it will read in files that have not yet been read. Defaults to '.*', " +
    "which indicates that no files will be filtered.";
  protected static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";
  protected static final String FORMAT_DESCRIPTION = "Format of the file. Must be 'text', 'avro' or " +
    "'parquet'. Defaults to 'text'.";

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  @VisibleForTesting
  static final long DEFAULT_MAX_SPLIT_SIZE = 134217728;

  @Nullable
  @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
  @Macro
  public String fileSystemProperties;

  @Nullable
  @Description(REGEX_DESCRIPTION)
  @Macro
  public String fileRegex;

  @Nullable
  @Description(TABLE_DESCRIPTION)
  @Macro
  public String timeTable;

  @Nullable
  @Description(INPUT_FORMAT_CLASS_DESCRIPTION)
  @Macro
  public String inputFormatClass;

  @Nullable
  @Description(FORMAT_DESCRIPTION)
  public String format;

  @Nullable
  @Description(MAX_SPLIT_SIZE_DESCRIPTION)
  @Macro
  public Long maxSplitSize;

  @Nullable
  @Description("Identify if path needs to be ignored or not, for case when directory or file does not exists. If " +
    "set to true it will treat the not present folder as zero input and log a warning. Default is false.")
  public Boolean ignoreNonExistingFolders;

  @Nullable
  @Description("Boolean value to determine if files are to be read recursively from the path. Default is false.")
  public Boolean recursive;

  @Nullable
  @Description("If specified, each output record will include a field with this name that contains the file URI " +
    "that the record was read from. Requires a customized version of CombineFileInputFormat, so it cannot be used " +
    "if an inputFormatClass is given.")
  public String pathField;

  @Nullable
  @Description("If true and a pathField is specified, only the filename will be used. If false, the full " +
    "URI will be used. Defaults to false.")
  public Boolean filenameOnly;

  @Nullable
  @Description("Schema for the source")
  public String schema;

  // this is a hidden property that only exists for wrangler's parse-as-csv that uses the header as the schema
  // when this is true and the format is text, the header will be the first record returned by every record reader
  @Nullable
  private Boolean copyHeader;

  public FileSourceConfig() {
    this(null, null, null, null, null, null, null, null, null, null, null, null);
  }

  public FileSourceConfig(String referenceName, @Nullable String fileRegex, @Nullable String timeTable,
                          @Nullable String inputFormatClass, @Nullable String fileSystemProperties,
                          @Nullable String format, @Nullable Long maxSplitSize,
                          @Nullable Boolean ignoreNonExistingFolders, @Nullable Boolean recursive,
                          @Nullable String pathField, @Nullable Boolean fileNameOnly,
                          @Nullable String schema) {
    super(referenceName);
    this.fileSystemProperties = fileSystemProperties == null ? GSON.toJson(ImmutableMap.<String, String>of()) :
      fileSystemProperties;
    this.fileRegex = fileRegex == null ? ".*" : fileRegex;
    // There is no default for timeTable, the code handles nulls
    this.timeTable = timeTable;
    this.inputFormatClass = inputFormatClass == null ?
      CombinePathTrackingInputFormat.class.getName() : inputFormatClass;
    this.format = format == null ? "text" : format;
    this.maxSplitSize = maxSplitSize == null ? DEFAULT_MAX_SPLIT_SIZE : maxSplitSize;
    this.ignoreNonExistingFolders = ignoreNonExistingFolders == null ? false : ignoreNonExistingFolders;
    this.recursive = recursive == null ? false : recursive;
    this.filenameOnly = fileNameOnly == null ? false : fileNameOnly;
    this.pathField = pathField;
    this.schema = schema;
    this.copyHeader = false;
  }

  protected void validate() {
    getFileSystemProperties();
    if (!CombinePathTrackingInputFormat.class.getName().equals(inputFormatClass) && pathField != null) {
      throw new IllegalArgumentException("pathField can only be used if inputFormatClass is " +
                                           CombinePathTrackingInputFormat.class.getName());
    }
    if (format == null || !(format.equalsIgnoreCase("text") ||
      (format.equalsIgnoreCase("avro")) || format.equalsIgnoreCase("parquet"))) {
      throw new IllegalArgumentException("Format can only be 'text', 'avro' or 'parquet'");
    }
    if (schema == null) {
      return;
    }
    Schema parsedSchema = getSchema();

    if (format.equalsIgnoreCase("text")) {
      Schema.Field offsetField = parsedSchema.getField("offset");
      if (offsetField == null) {
        throw new IllegalArgumentException("Schema for text format must have a field named 'offset'");
      }
      Schema offsetSchema = offsetField.getSchema();
      Schema.Type offsetType = offsetSchema.isNullable() ? offsetSchema.getNonNullable().getType() :
        offsetSchema.getType();
      if (offsetType != Schema.Type.LONG) {
        throw new IllegalArgumentException("Type of 'offset' field must be 'long', but found " + offsetType);
      }

      Schema.Field bodyField = parsedSchema.getField("body");
      if (bodyField == null) {
        throw new IllegalArgumentException("Schema for text format must have a field named 'body'");
      }
      Schema bodySchema = bodyField.getSchema();
      Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
      if (bodyType != Schema.Type.STRING) {
        throw new IllegalArgumentException("Type of 'body' field must be 'string', but found + " + bodyType);
      }
    }

    if (pathField != null) {
      Schema.Field schemaPathField = parsedSchema.getField(pathField);
      if (schemaPathField == null) {
        throw new IllegalArgumentException("No value specified for " + pathField);
      }
      Schema pathFieldSchema = schemaPathField.getSchema();
      Schema.Type pathFieldType = pathFieldSchema.isNullable() ? pathFieldSchema.getNonNullable().getType() :
        pathFieldSchema.getType();
      if (pathFieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException("Path field " + pathField + " must be of type 'string', but found " +
                                           pathFieldType);
      }
    }
  }

  protected Map<String, String> getFileSystemProperties() {
    if (fileSystemProperties == null) {
      return new HashMap<>();
    }
    try {
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse fileSystemProperties: " + e.getMessage());
    }
  }

  @Nullable
  protected Schema getSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
    }
  }

  protected abstract String getPath();

  boolean shouldCopyHeader() {
    return copyHeader;
  }
}
