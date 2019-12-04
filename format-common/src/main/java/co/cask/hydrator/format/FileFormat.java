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
import co.cask.hydrator.format.input.OrcInputProvider;
import co.cask.hydrator.format.input.ParquetInputProvider;
import co.cask.hydrator.format.input.TextInputProvider;
import co.cask.hydrator.format.output.AvroOutputProvider;
import co.cask.hydrator.format.output.DelimitedTextOutputProvider;
import co.cask.hydrator.format.output.FileOutputFormatter;
import co.cask.hydrator.format.output.FileOutputFormatterProvider;
import co.cask.hydrator.format.output.JsonOutputProvider;
import co.cask.hydrator.format.output.OrcOutputProvider;
import co.cask.hydrator.format.output.ParquetOutputProvider;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
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
  AVRO(new AvroInputProvider(), new AvroOutputProvider(), false),
  BLOB(new BlobInputProvider(), null, false),
  CSV(new DelimitedInputProvider(","), new DelimitedTextOutputProvider(","), false),
  DELIMITED(new DelimitedInputProvider(null), new DelimitedTextOutputProvider(null), false),
  JSON(new JsonInputProvider(), new JsonOutputProvider(), false),
  PARQUET(new ParquetInputProvider(), new ParquetOutputProvider(), true),
  TEXT(new TextInputProvider(), null, true),
  TSV(new DelimitedInputProvider("\t"), new DelimitedTextOutputProvider("\t"), false),
  ORC(new OrcInputProvider(), new OrcOutputProvider(), true);
  public final FileInputFormatterProvider inputProvider;
  private final FileOutputFormatterProvider outputProvider;
  private boolean getSchemaSupport = false;
  private final boolean canWrite;
  private final boolean canRead;


  FileFormat(@Nullable FileInputFormatterProvider inputProvider, @Nullable FileOutputFormatterProvider outputProvider, boolean getSchemaSupport) {
      this.inputProvider = inputProvider;
      this.outputProvider = outputProvider;
      this.canWrite = outputProvider != null;
      this.canRead = inputProvider != null;
      this.getSchemaSupport = getSchemaSupport;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public boolean canRead() {
    return canRead;
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
     * Returns one of the file present in user directory.
     *
     * @param filePath properties
     * @return the Actual File Path.
     * @throws IllegalArgumentException if the given filePath can't be opened by FileSystem
     */
   public static String getFilePath(String filePath, String regex) throws IOException{
       if (!Strings.isNullOrEmpty(filePath)) {
           Configuration configuration = new Configuration();
           Path path = new Path(filePath);
           FileSystem fs = FileSystem.get(configuration);
           if (fs.exists(path)) {
               if(fs.isDirectory(path)) {
                   RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, true);
                   while (iter.hasNext()) {
                       LocatedFileStatus fileStatus = iter.next();
                       if (fileStatus.getPath().getName().endsWith(regex)) {
                           return fileStatus.getPath().toString();
                       }
                   }
               } else {
                   if (filePath.endsWith(regex)) {
                       return filePath;
                   }
               }
           } else {
               throw new IllegalArgumentException("Invalid path " + filePath);
           }
       }
       return null;
   }


    /**
   * Return the schema for this format, if the format requires a specific schema. Returns null if the format does
   * not require a specific schema. Should only be called for formats that can read.
   *
   * @param pathField the field of the file path, if it exists.
   * @return the schema required by the format, if it exists
   */
  @Nullable
  public Schema getSchema(@Nullable String pathField, String filePath) {
    if (inputProvider == null) {
      throw new IllegalArgumentException(String.format("Format '%s' cannot be used for reading.", this.name()));
    }
    if (getSchemaSupport) {
        return inputProvider.getSchema(pathField, filePath);

    } else {
        throw new IllegalArgumentException("Get Schema Functionality is supported for Parquet and Orc format only");
    }
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
