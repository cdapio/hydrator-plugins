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
import co.cask.hydrator.format.output.AvroOutputProvider;
import co.cask.hydrator.format.output.DelimitedTextOutputProvider;
import co.cask.hydrator.format.output.FileOutputFormatter;
import co.cask.hydrator.format.output.FileOutputFormatterProvider;
import co.cask.hydrator.format.output.JsonOutputProvider;
import co.cask.hydrator.format.output.ParquetOutputProvider;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * FileFormat supported by the file based sources/sinks. Some formats can be used for both reading and writing.
 * Each value also contains an {@link FileOutputFormatterProvider} that contains the logic required to configure
 * a Hadoop OutputFormat for writing. This is meant to consolidate all format related operations so that multiple
 * plugins can easily support the same set of formats without re-implementing logic.
 */
public enum FileFormat {
  AVRO(new AvroOutputProvider()),
  BLOB(null),
  CSV(new DelimitedTextOutputProvider(",")),
  DELIMITED(new DelimitedTextOutputProvider(null)),
  JSON(new JsonOutputProvider()),
  PARQUET(new ParquetOutputProvider()),
  TEXT(null),
  TSV(new DelimitedTextOutputProvider("\t"));
  private final FileOutputFormatterProvider outputProvider;
  private final boolean canWrite;

  FileFormat(@Nullable FileOutputFormatterProvider outputProvider) {
    this.outputProvider = outputProvider;
    this.canWrite = outputProvider != null;
  }

  public boolean canWrite() {
    return canWrite;
  }

  /**
   * Create the FileOutputFormatter for this format.
   *
   * @param properties plugin properties
   * @param schema schema for the pipeline stage
   * @return the FileOutputFormatter for this format
   * @throws IllegalArgumentException if the properties or schema are not valid
   */
  public <K, V> FileOutputFormatter<K, V> getFileOutputFormatter(Map<String, String> properties,
                                                                 @Nullable Schema schema) {
    //noinspection unchecked
    if (outputProvider == null) {
      throw new IllegalArgumentException(String.format("Format '%s' cannot be used for writing", this.name()));
    }
    return (FileOutputFormatter<K, V>) outputProvider.create(properties, schema);
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
