/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Transformation that parses a field as CSV Record into {@link StructuredRecord}.
 *
 * <p>
 * CSVParser supports transforming the input into {@link StructuredRecord}
 * Following are different CSV record types that are supported by this transform.
 * <ul>
 *   <li>DEFAULT</li>
 *   <li>EXCEL</li>
 *   <li>RFC4180</li>
 *   <li>MYSQL and</li>
 *   <li>TDF</li>
 * </ul>
 * </p>  
 */
@Plugin(type = "transform")
@Name("CSVParser")
@Description("Parses a field as CSV Record into a Structured Record.")
public final class CSVParser extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVParser.class);
  private final Config config;

  // Output Schema associated with transform output. 
  private Schema outSchema;

  // List of fields specified in the schema. 
  private List<Field> fields;

  // Format of CSV.
  private CSVFormat csvFormat = CSVFormat.DEFAULT;

  // This is used only for tests, otherwise this is being injected by the ingestion framework. 
  public CSVParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);

    // Check if the format specified is valid.
    if (config.format == null || config.format.isEmpty()) {
      throw new IllegalArgumentException("Format is not specified. Allowed values are DEFAULT, EXCEL, MYSQL," +
                                           " RFC4180 & TDF");
    }

    // Check if format is one of the allowed types.
    if (!config.format.equalsIgnoreCase("DEFAULT") && !config.format.equalsIgnoreCase("EXCEL") &&
      !config.format.equalsIgnoreCase("MYSQL") && !config.format.equalsIgnoreCase("RFC4180") &&
      !config.format.equalsIgnoreCase("TDF")) {
      throw new IllegalArgumentException("Format specified is not one of the allowed values. Allowed values are " +
                                           "DEFAULT, EXCEL, MYSQL, RFC4180 & TDF");
    }

    // Check if schema specified is a valid schema or no.
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }

  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    String csvFormatString = config.format.toLowerCase();
    switch (csvFormatString) {
      case "default":
        csvFormat = CSVFormat.DEFAULT;
        break;

      case "excel":
        csvFormat = CSVFormat.EXCEL;
        break;

      case "mysql":
        csvFormat = CSVFormat.MYSQL;
        break;

      case "rfc4180":
        csvFormat = CSVFormat.RFC4180;
        break;

      case "tdf":
        csvFormat = CSVFormat.TDF;
        break;

      default:
        throw new IllegalArgumentException("Format {} specified is not one of the allowed format. Allowed formats are" +
                                             "DEFAULT, EXCEL, MYSQL, RFC4180 and TDF");
    }

    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    // Field has to string to be parsed correctly. For others throw an exception.
    String body = in.get(config.field);

    // Parse the text as CSV and emit it as structured record.
    try {
      org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVParser.parse(body, csvFormat);
      List<CSVRecord> records = parser.getRecords();
      for (CSVRecord record : records) {
        if (fields.size() == record.size()) {
          StructuredRecord sRecord = createStructuredRecord(record);
          emitter.emit(sRecord);
        } else {
          LOG.warn("Skipping record as ouput schema specified has '{}' fields, while CSV record has '{}'",
                   fields.size(), record.size());
          // Write the record to error Dataset.
        }
      }
    } catch (IOException e) {
      LOG.error("There was a issue parsing the record. ", e.getLocalizedMessage());
    }
  }

  private StructuredRecord createStructuredRecord(CSVRecord record) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    int i = 0;
    for (Field field : fields) {
      builder.set(field.getName(), TypeConvertor.get(record.get(i), field.getSchema().getType()));
      ++i;
    }
    return builder.build();
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {

    @Name("format")
    @Description("Specify one of the predefined formats. DEFAULT, EXCEL, MYSQL, RFC4180 & TDF are supported formats.")
    private final String format;

    @Name("field")
    @Description("Specify the field that should be used for parsing into CSV.")
    private final String field;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String format, String field, String schema) {
      this.format = format;
      this.field = field;
      this.schema = schema;
    }
  }
}
