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
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nullable;

/**
 * Decompreses the configured fields using the algorithms specified.
 */
@Plugin(type = "transform")
@Name("Decompressor")
@Description("Decompresses configured fields using the algorithms specified.")
public final class Decompressor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Decompressor.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Output Field name to type map
  private Map<String, Schema.Type> outSchemaMap = new HashMap<>();

  // Map of field to decompressor type.
  private final Map<String, DecompressorType> deCompMap = new TreeMap<>();

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Decompressor(Config config) {
    this.config = config;
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    parseConfiguration(config.decompressor, context.getFailureCollector());

    // Initialize the required member maps and then have a one-to-one transform on all fields present in
    // outSchemaMap and deCompMap and aren't set to NONE as decompressorType.
    List<String> inFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    List<String> outFields = TransformLineageRecorderUtils.getFields(context.getOutputSchema());
    List<String> identityFields = outFields.stream()
      .filter(field -> !deCompMap.containsKey(field) || deCompMap.get(field) == DecompressorType.NONE)
      .collect(Collectors.toList());

    List<String> processedFields = new ArrayList<>(outFields);
    processedFields.removeAll(identityFields);
    List<String> droppedFields = new ArrayList<>(inFields);
    droppedFields.removeAll(outFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(processedFields, "decompress",
      "Used the specified algorithm to decompress the field."));
    output.addAll(TransformLineageRecorderUtils.generateDrops(droppedFields));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    parseConfiguration(config.decompressor, collector);

    Schema outputSchema = config.getSchema(collector);
    List<Field> outFields = outputSchema.getFields();
    for (Field field : outFields) {
      outSchemaMap.put(field.getName(), field.getSchema().getType());
    }
    validateInputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema(), collector);

    for (Map.Entry<String, DecompressorType> entry : deCompMap.entrySet()) {
      String fieldName = entry.getKey();
      if (!outSchemaMap.containsKey(fieldName)) {
        collector.addFailure(String.format("Decompress field '%s' must be present in output schema.", fieldName),
                             "Add decompress field in output schema or remove it from decompress.")
          .withConfigElement(Config.NAME_DECOMPRESSOR, fieldName + Config.SEPARATOR + entry.getValue());
        continue;
      }

      Schema fieldSchema = outputSchema.getField(fieldName).getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

      if (outSchemaMap.get(fieldName) != Schema.Type.BYTES && outSchemaMap.get(fieldName) != Schema.Type.STRING) {
        collector.addFailure(String.format("Field '%s' is of unexpected type '%s'.",
                                           fieldName, fieldSchema.getDisplayName()),
                             "It must be of type 'bytes' or 'string'")
          .withOutputSchemaField(fieldName).withConfigElement(Config.NAME_DECOMPRESSOR,
                                                              fieldName + Config.SEPARATOR + entry.getValue());
      }
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    parseConfiguration(config.decompressor, collector);
    collector.getOrThrowException();

    try {
      outSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outSchema.getFields();
      for (Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format." +
                                           e.getMessage());
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);

    Schema inSchema = in.getSchema();
    List<Field> inFields = inSchema.getFields();

    // Iterate through input fields. Check if field name is present
    // in the fields that need to be decompressed, if it's not then write
    // to output as it is.
    for (Field field : inFields) {
      String name = field.getName();

      // Check if output schema also have the same field name. If it's not
      // then continue.
      if (!outSchemaMap.containsKey(name)) {
        continue;
      }

      Schema.Type outFieldType = outSchemaMap.get(name);

      // Check if the input field name is configured to be encoded. If the field is not
      // present or is defined as none, then pass through the field as is.
      if (!deCompMap.containsKey(name) || deCompMap.get(name) == DecompressorType.NONE) {
        builder.set(name, in.get(name));
      } else {
        // Now, the input field should be of type byte[]
        byte[] obj = new byte[0];
        if (field.getSchema().getType() == Schema.Type.BYTES) {
          obj = in.get(name);
        } else {
          LOG.error("Input field '" + name + "' should be of type BYTES to decompress. It is currently of type " +
                      "'" + field.getSchema().getType().toString() + "'");
          break;
        }

        // Now, based on the encode type configured for the field - encode the byte[] of the
        // value.
        byte[] outValue = new byte[0];
        DecompressorType type = deCompMap.get(name);
        if (type == DecompressorType.SNAPPY) {
          outValue = Snappy.uncompress(obj);
        } else if (type == DecompressorType.ZIP) {
          outValue = unzip(obj);
        } else if (type == DecompressorType.GZIP) {
          outValue = ungzip(obj);
        }

        // Depending on the output field type, either convert it to
        // Bytes or to String.
        if (outValue != null) {
          if (outFieldType == Schema.Type.BYTES) {
            builder.set(name, outValue);
          } else if (outFieldType == Schema.Type.STRING) {
            builder.set(name, new String(outValue, "UTF-8"));
          }
        }
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Decompresses using GZIP Algorithm. 
   */
  private byte[] ungzip(byte[] body) {
    ByteArrayInputStream bytein = new ByteArrayInputStream(body);
    try (GZIPInputStream gzin = new GZIPInputStream(bytein);
         ByteArrayOutputStream byteout = new ByteArrayOutputStream()) {
      int res = 0;
      byte buf[] = new byte[1024];
      while (res >= 0) {
        res = gzin.read(buf, 0, buf.length);
        if (res > 0) {
          byteout.write(buf, 0, res);
        }
      }
      byte uncompressed[] = byteout.toByteArray();
      return uncompressed;
    } catch (IOException e) {
      // Most of operations here are in memory. So, we shouldn't get here.
      // Logging here is not an option.
    }
    return null;
  }

  /**
   * Decompresses using ZIP Algorithm.
   */
  private byte[] unzip(byte[] body)  {
    ZipEntry ze;
    byte buf[] = new byte[1024];
    try (ByteArrayOutputStream bao = new ByteArrayOutputStream();
         ByteArrayInputStream bytein = new ByteArrayInputStream(body);
         ZipInputStream zis = new ZipInputStream(bytein)) {
      while ((ze = zis.getNextEntry()) != null) {
        int l = 0;
        while ((l = zis.read(buf)) > 0) {
          bao.write(buf, 0, l);
        }
      }
      return bao.toByteArray();
    } catch (IOException e) {
      // Most of operations here are in memory. So, we shouldn't get here.
      // Logging here is not an option.       
    }
    return null;
  }

  /**
   * Enum specifying the decompressor type.
   */
  private enum DecompressorType {
    SNAPPY("SNAPPY"),
    ZIP("ZIP"),
    GZIP("GZIP"),
    NONE("NONE");

    private String type;

    DecompressorType(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  private void parseConfiguration(String config, FailureCollector collector) {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(Config.SEPARATOR);

      // If format is not right, then we throw an exception.
      if (params.length < 2) {
        collector.addFailure(String.format("Configuration '%s' is incorrectly formed.", mapping),
                             "Specify the configuration in the format <fieldname>:<decompressor-type>.")
          .withConfigProperty(Config.NAME_DECOMPRESSOR);
        continue;
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      DecompressorType cType = DecompressorType.valueOf(type);

      if (deCompMap.containsKey(field)) {
        collector.addFailure(String.format("Field '%s' already has decompressor set.", field),
                             "Ensure different fields are provided.")
          .withConfigElement(Config.NAME_DECOMPRESSOR, mapping);
        continue;
      }

      deCompMap.put(field, cType);
    }
  }

  private void validateInputSchema(@Nullable Schema inputSchema, FailureCollector collector) {
    if (inputSchema == null) {
      return;
    }

    for (Field field : inputSchema.getFields()) {
      String fieldName = field.getName();
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();
      Schema.Type type = nonNullableSchema.getType();
      Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();

      if (outSchemaMap.containsKey(fieldName) &&
        deCompMap.containsKey(fieldName) && deCompMap.get(fieldName) != DecompressorType.NONE &&
        (!Schema.Type.BYTES.equals(type) || logicalType != null)) {
        collector.addFailure(String.format("Input field '%s' is of unsupported type '%s'.",
                                           field.getName(), nonNullableSchema.getDisplayName()),
                             "Supported type is bytes.")
          .withInputSchemaField(field.getName())
          .withConfigElement(Config.NAME_DECOMPRESSOR, fieldName + Config.SEPARATOR + deCompMap.get(fieldName));
      }
    }
  }

  /**
   * Decompressor Plugin configuration.
   */
  public static class Config extends PluginConfig {
    private static final String NAME_DECOMPRESSOR = "decompressor";
    private static final String NAME_SCHEMA = "schema";
    private static final String SEPARATOR = ":";

    @Name(NAME_DECOMPRESSOR)
    @Description("Specify the field and decompression type combination. " +
      "Format is <field>:<decompressor-type>[,<field>:<decompressor-type>]*")
    private final String decompressor;

    @Name(NAME_SCHEMA)
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String decompressor, String schema) {
      this.decompressor = decompressor;
      this.schema = schema;
    }

    private Schema getSchema(FailureCollector collector) {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema : " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
      }
      throw collector.getOrThrowException();
    }
  }
}
