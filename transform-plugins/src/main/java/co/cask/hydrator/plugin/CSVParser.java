/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import com.google.common.base.Throwables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

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
 *   <li>MYSQL</li>
 *   <li>TDF and</li>
 *   <li>PDL</li>   
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

  // Format of PDL.
  public static final CSVFormat PDL;

  // Initialize Pipe Delimiter CSV Parser. 
  static {
    PDL = CSVFormat.DEFAULT.withDelimiter('|').withEscape('\\').withIgnoreEmptyLines(false)
      .withAllowMissingColumnNames().withQuote((Character) null).withRecordSeparator('\n')
      .withIgnoreSurroundingSpaces();
  }

  // This is used only for tests, otherwise this is being injected by the ingestion framework. 
  public CSVParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    config.validate();
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    validateInputSchema(inputSchema);
    configurer.getStageConfigurer().setOutputSchema(parseAndValidateOutputSchema(inputSchema));
  }

  void validateInputSchema(Schema inputSchema) {
    if (inputSchema != null) {
      // Check the existence of field in input schema
      Schema.Field inputSchemaField = inputSchema.getField(config.field);
      if (inputSchemaField == null) {
        throw new IllegalArgumentException(
          "Field " + config.field + " is not present in the input schema");
      }

      // Check that the field type is String or Nullable String
      Schema fieldSchema = inputSchemaField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!fieldType.equals(Schema.Type.STRING)) {
        throw new IllegalArgumentException(
          "Type for field  " + config.field + " must be String");
      }
    }
  }

  Schema parseAndValidateOutputSchema(Schema inputSchema) {
    // Check if schema specified is a valid schema or no.
    try {
      Schema outputSchema = Schema.parseJson(this.config.schema);

      // When a input field is passed through to output, the type and name should be the same.
      // If the type is not the same, then we fail.
      if (inputSchema != null) {
        for (Field field : inputSchema.getFields()) {
          if (outputSchema.getField(field.getName()) != null) {
            Schema out = outputSchema.getField(field.getName()).getSchema();
            Schema in = field.getSchema();
            if (!in.equals(out)) {
              throw new IllegalArgumentException(
                "Input field '" + field.getName() + "' does not have same output schema as input."
              );
            }
          }
        }
      }
      return outputSchema;
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    config.validate();

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

      case "pdl":
        csvFormat = PDL;
        break;

      default:
        throw new IllegalArgumentException("Format {} specified is not one of the allowed format. Allowed formats are" +
                                             "DEFAULT, EXCEL, MYSQL, RFC4180, PDL and TDF");
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
      if (body == null) {
        emitter.emit(createStructuredRecord(null, in));
      } else {
        org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVParser.parse(body, csvFormat);
        List<CSVRecord> records = parser.getRecords();
        for (CSVRecord record : records) {
          emitter.emit(createStructuredRecord(record, in));
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private StructuredRecord createStructuredRecord(@Nullable CSVRecord record, StructuredRecord in) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    int i = 0;
    for (Field field : fields) {
      String name = field.getName();
      // If the field specified in the output field is present in the input, then
      // it's directly copied into the output, else field is parsed in from the CSV parser.
      // If the input record is null, propagate all supplied input fields and null other fields
      // assumed to be CSV-parsed fields
      if (in.get(name) != null) {
        builder.set(name, in.get(name));
      } else if (record == null) {
        builder.set(name, null);
      } else {
        String val = record.get(i);
        Schema fieldSchema = field.getSchema();

        if (val.isEmpty()) {
          boolean isNullable = fieldSchema.isNullable();
          Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
          // if the field is a string or a nullable string, set the value to the empty string
          if (fieldType == Schema.Type.STRING) {
            builder.set(field.getName(), "");
          } else if (!isNullable) {
            // otherwise, error out
            throw new IllegalArgumentException(String.format(
              "Field #%d (named '%s') is of non-nullable type '%s', " +
                "but was parsed as an empty string for CSV record '%s'",
              i, field.getName(), field.getSchema().getType(), record));
          }
        } else {
          builder.convertAndSet(field.getName(), val);
        }
        ++i;
      }
    }
    return builder.build();
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {

    @Name("format")
    @Description("Specify one of the predefined formats. DEFAULT, EXCEL, MYSQL, RFC4180, PDL & TDF " +
      "are supported formats.")
    private final String format;

    @Name("field")
    @Description("Specify the field that should be used for parsing into CSV. Input records with a null input field " +
      "propagate all other fields and set fields that would otherwise be parsed by the CSVParser to null.")
    private final String field;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String format, String field, String schema) {
      this.format = format;
      this.field = field;
      this.schema = schema;
    }

    private void validate() {
      // Check if the format specified is valid.
      if (format == null || format.isEmpty()) {
        throw new IllegalArgumentException("Format is not specified. Allowed values are DEFAULT, EXCEL, MYSQL," +
                                             " RFC4180, PDL & TDF");
      }

      // Check if format is one of the allowed types.
      if (!format.equalsIgnoreCase("DEFAULT") && !format.equalsIgnoreCase("EXCEL") &&
        !format.equalsIgnoreCase("MYSQL") && !format.equalsIgnoreCase("RFC4180") &&
        !format.equalsIgnoreCase("TDF") && !format.equalsIgnoreCase("PDL")) {
        throw new IllegalArgumentException("Format specified is not one of the allowed values. Allowed values are " +
                                             "DEFAULT, EXCEL, MYSQL, RFC4180, PDL & TDF");
      }
    }
  }
}
