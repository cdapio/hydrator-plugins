/*
 * Copyright © 2018-2021 Cask Data, Inc.
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

package io.cdap.plugin.format.parquet.input;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.InputFile;
import io.cdap.cdap.etl.api.validation.InputFiles;
import io.cdap.cdap.etl.api.validation.SeekableInputStream;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides and sets up configuration for an parquet input format.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(ParquetInputFormatProvider.NAME)
@Description(ParquetInputFormatProvider.DESC)
public class ParquetInputFormatProvider extends PathTrackingInputFormatProvider<ParquetInputFormatProvider.Conf> {
  static final String NAME = "parquet";
  static final String DESC = "Plugin for reading files in text format.";
  public static final PluginClass PLUGIN_CLASS =
    new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, ParquetInputFormatProvider.class.getName(),
                    "conf", PathTrackingConfig.FIELDS);

  public ParquetInputFormatProvider(ParquetInputFormatProvider.Conf conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return CombineParquetInputFormat.class.getName();
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    Schema schema = conf.getSchema();
    if (schema != null) {
      properties.put("parquet.avro.read.schema", schema.toString());
    }
  }

  @Nullable
  @Override
  public Schema detectSchema(FormatContext context, InputFiles inputFiles) throws IOException {
    ParquetReadOptions readOptions = ParquetReadOptions.builder().build();
    for (InputFile inputFile : inputFiles) {
      if (!inputFile.getName().toLowerCase().endsWith(".parquet")) {
        continue;
      }
      try (ParquetFileReader parquetFileReader = ParquetFileReader.open(new HadoopInputFile(inputFile), readOptions)) {
        MessageType parquetSchema = parquetFileReader.getFooter().getFileMetaData().getSchema();
        return new AvroToStructuredTransformer().convertSchema(new AvroSchemaConverter().convert(parquetSchema));
      }
    }
    throw new IOException("Unable to find any files that end with .parquet");
  }

  private static class HadoopInputFile implements org.apache.parquet.io.InputFile {
    private final InputFile file;

    HadoopInputFile(InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
      SeekableInputStream inputStream = file.open();
      return new DelegatingSeekableInputStream(inputStream) {

        @Override
        public long getPos() throws IOException {
          return inputStream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
          inputStream.seek(newPos);
        }
      };
    }
  }

  /**
   * Common config for Parquet format
   */
  public static class Conf extends PathTrackingConfig {

    @Macro
    @Nullable
    @Description(NAME_SCHEMA)
    public String schema;
  }
}
