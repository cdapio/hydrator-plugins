/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transform that formats a {@link StructuredRecord} to CSV.
 * <p>
 * CSVFormatter supports transforming the input {@link StructuredRecord}
 * into CSV Record of varying types. Following are different CSV record
 * types that are supported by this transform.
 * <ul>
 *   <li>DELIMITED</li>
 *   <li>EXCEL</li>
 *   <li>RFC4180</li>
 *   <li>MYSQL and</li>
 *   <li>TDF</li>
 * </ul>
 * </p>
 */
@Plugin(type = "transform")
@Name("CSVFormatter")
@Description("Formats a Structured Record to CSV")
public final class CSVFormatter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVFormatter.class);

  // Transform configuration.
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // List of fields specified in the schema.
  private List<Field> fields;

  // Mapping from delimiter name to the character to be used as delimiter.
  private static final Map<String, String> delimMap = Maps.newHashMap();

  // Format of CSV File.
  private CSVFormat csvFileFormat;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public CSVFormatter(Config config) {
    this.config = config;
  }

  // Static collection of delimiter mappings from name to delim.
  static {
    delimMap.put("COMMA", ",");
    delimMap.put("CTRL-A", "\001");
    delimMap.put("TAB", "\t");
    delimMap.put("VBAR", "|");
    delimMap.put("STAR", "*");
    delimMap.put("CARET", "^");
    delimMap.put("DOLLAR", "$");
    delimMap.put("HASH", "#");
    delimMap.put("TILDE", "~");
    delimMap.put("CTRL-B", "\002");
    delimMap.put("CTRL-C", "\003");
    delimMap.put("CTRL-D", "\004");
    delimMap.put("CTRL-E", "\005");
    delimMap.put("CTRL-F", "\006");
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    Schema schema = config.getSchema(collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);

    // Map all the input fields to the first output field.
    List<String> outputFields = TransformLineageRecorderUtils.getFields(context.getOutputSchema());
    if (!outputFields.isEmpty()) {
      context.record(
        TransformLineageRecorderUtils
          .generateManyToOne(TransformLineageRecorderUtils.getFields(context.getInputSchema()), outputFields.get(0),
            "csvFormat", "Formatted the input data as CSV."));
    }
    config.validate(context.getFailureCollector());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }

    // Based on the delimiter name specified pick the delimiter to be used for the record.
    // This is only applicable when the format type is chosen as DELIMITER
    char delim;
    if (delimMap.containsKey(config.delimiter)) {
      delim = delimMap.get(config.delimiter).charAt(0);
    } else {
      throw new IllegalArgumentException("Unknown delimiter '" + config.delimiter + "' specified. ");
    }

    // Create CSVFileFormat based on the format specified.
    switch (config.format.toLowerCase()) {
      case "delimited":
        csvFileFormat = CSVFormat.newFormat(delim).withQuote('"')
          .withRecordSeparator("\r\n").withIgnoreEmptyLines();
        break;

      case "excel":
        csvFileFormat = CSVFormat.Predefined.Excel.getFormat();
        break;

      case "mysql":
        csvFileFormat = CSVFormat.Predefined.MySQL.getFormat();
        break;

      case "tdf":
        csvFileFormat = CSVFormat.Predefined.TDF.getFormat();
        break;

      case "rfc4180":
        csvFileFormat = CSVFormat.Predefined.RFC4180.getFormat();
        break;

      default:
        throw new RuntimeException("Unknown format specified for CSV. Please check the format.");
    }
  }

  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    List<Object> values = Lists.newArrayList();
    for (Schema.Field field : record.getSchema().getFields()) {
      values.add(record.get(field.getName()));
    }

    CSVPrinter printer = new CSVPrinter(new StringWriter(), csvFileFormat);
    if (printer != null) {
      printer.printRecord(values);
      emitter.emit(StructuredRecord.builder(outSchema)
                     .set(outSchema.getFields().get(0).getName(), printer.getOut().toString())
                     .build());
      printer.close();
    }
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    private static final String NAME_FORMAT = "format";
    private static final String NAME_DELIMITER = "delimiter";
    private static final String NAME_SCHEMA = "schema";

    @Name(NAME_FORMAT)
    @Description("Specify one of the predefined formats. DEFAULT, EXCEL, MYSQL, RFC4180 & TDF are supported formats.")
    private final String format;

    @Name(NAME_DELIMITER)
    @Description("Specify delimiter to be used for separating fields.")
    private final String delimiter;

    @Name(NAME_SCHEMA)
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String format, String delimiter, String schema) {
      this.format = format;
      this.delimiter = delimiter;
      this.schema = schema;
    }

    @VisibleForTesting
    void validate(FailureCollector collector) {
      if (!delimMap.containsKey(delimiter)) {
        collector.addFailure(String.format("Delimiter '%s' is unsupported.", delimiter),
                             "Specify one of the following: " + Joiner.on(", ").join(delimMap.keySet()))
          .withConfigProperty(NAME_DELIMITER);
      }

      // Check if the format specified is valid.
      if (Strings.isNullOrEmpty(format)) {
        collector.addFailure("Format must be specified.", null)
          .withConfigProperty(NAME_FORMAT);
      } else {
        if (!format.equalsIgnoreCase("DELIMITED") && !format.equalsIgnoreCase("EXCEL") &&
          !format.equalsIgnoreCase("MYSQL") && !format.equalsIgnoreCase("RFC4180") &&
          !format.equalsIgnoreCase("TDF")) {
          collector.addFailure(String.format("Format '%s' is not supported.", format),
                               "Supported formats are : DELIMITED, EXCEL, MYSQL, RFC4180 & TDF")
            .withConfigProperty(NAME_FORMAT);
        }
      }

      Schema schema = getSchema(collector);
      if (schema != null) {
        List<Schema.Field> fields = schema.getFields();
        if (fields.size() > 1) {
          // Add a validation failure for each extra field considering first field is the correct field
          for (int i = 1; i < fields.size(); i++) {
            collector.addFailure("Output schema must only contain single field of type 'string'.",
                                 String.format("Remove '%s' field.", fields.get(i).getName()))
              .withOutputSchemaField(fields.get(i).getName());
          }
        }

        Schema nonNullableSchema = fields.get(0).getSchema().isNullable() ?
          fields.get(0).getSchema().getNonNullable() : fields.get(0).getSchema();

        if (nonNullableSchema.getType() != Schema.Type.STRING) {
          collector.addFailure(String.format("Output field '%s' is of invalid type '%s'.",
                                             fields.get(0).getName(), nonNullableSchema.getDisplayName()),
                               "Specify output field of type 'string'.")
            .withOutputSchemaField(fields.get(0).getName(), null);
        }
      }
      collector.getOrThrowException();
    }

    @Nullable
    private Schema getSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(this.schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
      }
      throw collector.getOrThrowException();
    }
  }
}

