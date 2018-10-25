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

package co.cask.hydrator.format;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.format.input.AvroInputProvider;
import co.cask.hydrator.format.input.BlobInputProvider;
import co.cask.hydrator.format.input.DelimitedInputProvider;
import co.cask.hydrator.format.input.FileInputFormatter;
import co.cask.hydrator.format.input.FileInputFormatterProvider;
import co.cask.hydrator.format.input.JsonInputProvider;
import co.cask.hydrator.format.input.ParquetInputProvider;
import co.cask.hydrator.format.input.TextInputProvider;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * FileFormat supported by the file based sources/sinks. Some formats can be used for both reading and writing.
 * TODO: remove once formats have completely been converted to plugins
 */
public enum FileFormat {
  AVRO(new AvroInputProvider(), true),
  BLOB(new BlobInputProvider(), false),
  CSV(new DelimitedInputProvider(","), true),
  DELIMITED(new DelimitedInputProvider(null), true),
  JSON(new JsonInputProvider(), true),
  ORC(null, true),
  PARQUET(new ParquetInputProvider(), true),
  TEXT(new TextInputProvider(), false),
  TSV(new DelimitedInputProvider("\t"), true);
  private final FileInputFormatterProvider inputProvider;
  private final boolean canWrite;
  private final boolean canRead;

  FileFormat(@Nullable FileInputFormatterProvider inputProvider, boolean canWrite) {
    this.inputProvider = inputProvider;
    this.canRead = inputProvider != null;
    this.canWrite = canWrite;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public boolean canRead() {
    return canRead;
  }

  /**
   * Create the FileInputFormatter for this format.
   *
   * @param properties plugin properties
   * @param schema schema for the pipeline stage
   * @return the FileInputFormatter for this format
   * @throws IllegalArgumentException if the properties or schema are not valid
   */
  public FileInputFormatter getFileInputFormatter(Map<String, String> properties, @Nullable Schema schema) {
    if (inputProvider == null) {
      throw new IllegalArgumentException(String.format("Format '%s' cannot be used for reading.", this.name()));
    }
    return inputProvider.create(properties, schema);
  }

  /**
   * Return the schema for this format, if the format requires a specific schema. Returns null if the format does
   * not require a specific schema. Should only be called for formats that can read.
   *
   * @param pathField the field of the file path, if it exists.
   * @return the schema required by the format, if it exists
   */
  @Nullable
  public Schema getSchema(@Nullable String pathField) {
    if (inputProvider == null) {
      throw new IllegalArgumentException(String.format("Format '%s' cannot be used for reading.", this.name()));
    }
    return inputProvider.getSchema(pathField);
  }

  /**
   * Get a FileFormat from the specified string. This is similar to the valueOf method except that the error
   * message will contain the full set of valid values. It also supports filtering which enum values are valid.
   * This can be used to only get FileFormats that can be used for reading or only get formats that can be used
   * for writing.
   *
   * @param format the format to get
   * @param isValidFormat a filter used to only allow certain enum values
   * @return the FileFormat corresponding to the specified string
   * @throws IllegalArgumentException if the specified format does not have an equivalent value that also satisfies the
   *   specified predicate
   */
  public static FileFormat from(String format, Predicate<FileFormat> isValidFormat) {
    FileFormat fileFormat;
    try {
      fileFormat = valueOf(format.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(getExceptionMessage(format, isValidFormat));
    }

    if (!isValidFormat.test(fileFormat)) {
      throw new IllegalArgumentException(getExceptionMessage(format, isValidFormat));
    }

    return fileFormat;
  }

  /**
   * Return an error message that enumerates all valid values that are acceptable.
   */
  private static String getExceptionMessage(String format, Predicate<FileFormat> isValid) {
    String values = Arrays.stream(FileFormat.values())
      .filter(isValid)
      .map(f -> f.name().toLowerCase())
      .collect(Collectors.joining(", "));
    throw new IllegalArgumentException(String.format("Invalid format '%s'. The value must be one of %s",
                                                     format, values));
  }
}
