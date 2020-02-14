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

import com.google.common.base.Strings;
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
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Decodes the input fields as BASE64, BASE32 or HEX.
 * Please note that Encoder and Decoder might look the same right now, but in near future they will diverge.
 */
@Plugin(type = "transform")
@Name("Decoder")
@Description("Decodes the input field(s) using Base64, Base32, or Hex")
public final class Decoder extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  // Mapping of input field to decoder type.
  private final Map<String, DecoderType> decodeMap = new TreeMap<>();
  // Decoder handlers.
  private final Base64 base64Decoder = new Base64();
  private final Base32 base32Decoder = new Base32();
  private final Hex hexDecoder = new Hex();
  // Output Field name to type map
  private final Map<String, Schema.Type> outSchemaMap = new HashMap<>();
  // Output Schema associated with transform output.
  private Schema outSchema;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Decoder(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    parseConfiguration(config.decode, collector);
    validateInputSchema(stageConfigurer.getInputSchema(), collector);

    Schema outputSchema = config.getSchema(collector);
    if (outputSchema != null) {
      for (String fieldName : decodeMap.keySet()) {
        if (outputSchema.getField(fieldName) == null) {
          collector.addFailure(String.format("Field '%s' must be in output schema.", fieldName), null)
            .withConfigElement(Config.NAME_DECODE, fieldName + Config.SEPARATOR + decodeMap.get(fieldName));
          continue;
        }

        Field field = outputSchema.getField(fieldName);
        Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
          : field.getSchema();
        if (nonNullableSchema.getType() != Schema.Type.BYTES && nonNullableSchema.getType() != Schema.Type.STRING
          || nonNullableSchema.getLogicalType() != null) {
          collector.addFailure(String.format("Decode field '%s' is of invalid type '%s'.",
                                             field.getName(), nonNullableSchema.getDisplayName()),
                               "Ensure the decode field is of type string or bytes.")
            .withOutputSchemaField(field.getName())
            .withConfigElement(Config.NAME_DECODE, fieldName + Config.SEPARATOR + decodeMap.get(fieldName));
        }
      }
    }

    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    parseConfiguration(config.decode, context.getFailureCollector());

    // Initialize the required member maps and then: if a field is in input and output and decode, then
    // set it to transform; if it's in input and output but not decode, ignore it; if it's in input and
    // not output, drop it.
    List<String> inFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    List<String> outFields = TransformLineageRecorderUtils.getFields(context.getOutputSchema());
    List<String> identityFields = outFields.stream()
      .filter(field -> !decodeMap.containsKey(field) || decodeMap.get(field) == DecoderType.NONE)
      .collect(Collectors.toList());

    List<String> processedFields = new ArrayList<>(outFields);
    processedFields.removeAll(identityFields);
    List<String> droppedFields = new ArrayList<>(inFields);
    droppedFields.removeAll(outFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(processedFields, "decode",
      "Decoded the input fields based on expected decoder."));
    output.addAll(TransformLineageRecorderUtils.generateDrops(droppedFields));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    parseConfiguration(config.decode, collector);
    collector.getOrThrowException();

    try {
      outSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outSchema.getFields();
      for (Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    Schema inSchema = in.getSchema();
    List<Field> inFields = inSchema.getFields();
    // Iterate through input fields. Check if field name is present
    // in the fields that need to be decoded, if it's not then write
    // to output as it is.
    for (Field field : inFields) {
      String name = field.getName();

      // Check if the output schema has the field, if it's not there
      // then skip and move to processing the next field.
      if (!outSchemaMap.containsKey(name)) {
        continue;
      }
      Schema.Type outFieldType = outSchemaMap.get(name);

      // Check if the input field name is configured to be decoded. If the field is not
      // present or is defined as none, then pass through the field as is.
      if (!decodeMap.containsKey(name) || decodeMap.get(name) == DecoderType.NONE) {
        builder.set(name, in.get(name));
      } else {
        // Now, the input field could be of type String or byte[], so transform everything
        // to byte[]
        byte[] obj = new byte[0];
        if (field.getSchema().getType() == Schema.Type.STRING) {
          obj = ((String) in.get(name)).getBytes();
        } else if (field.getSchema().getType() == Schema.Type.BYTES) {
          obj = in.get(name);
        }

        // Now, based on the decode type configured for the field - decode the byte[] of the
        // value.
        byte[] outValue = new byte[0];
        DecoderType type = decodeMap.get(name);
        if (type == DecoderType.STRING_BASE32 || type == DecoderType.BASE32) {
          outValue = base32Decoder.decode(obj);
        } else if (type == DecoderType.STRING_BASE64 || type == DecoderType.BASE64) {
          outValue = base64Decoder.decode(obj);
        } else if (type == DecoderType.HEX) {
          outValue = hexDecoder.decode(obj);
        }

        // Depending on the output field type, either convert it to
        // Bytes or to String.
        if (outFieldType == Schema.Type.BYTES) {
          builder.set(name, outValue);
        } else if (outFieldType == Schema.Type.STRING) {
          builder.set(name, new String(outValue, "UTF-8"));
        }
      }
    }
    emitter.emit(builder.build());
  }

  private void parseConfiguration(String config, FailureCollector collector) {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(":");

      // If format is not right, then we throw an exception.
      if (params.length < 2) {
        collector.addFailure(String.format("Configuration '%s' is incorrectly formed.", mapping),
                             "Specify the configuration in the format <fieldname>:<decoder-type>.")
          .withConfigProperty(Config.NAME_DECODE);
        continue;
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      DecoderType eType = DecoderType.valueOf(type);

      if (decodeMap.containsKey(field)) {
        collector.addFailure(String.format("Field '%s' already has decoder set.", field),
                             "Ensure different fields are provided.")
          .withConfigElement(Config.NAME_DECODE, mapping);
        continue;
      }

      decodeMap.put(field, eType);
    }
  }

  private void validateInputSchema(@Nullable Schema inputSchema, FailureCollector collector) {
    if (inputSchema == null) {
      return;
    }
    // for the fields in input schema, if they are to be decoded (if present in decodeMap)
    // make sure their type is either String or Bytes and throw exception otherwise
    for (Field field : inputSchema.getFields()) {
      if (decodeMap.containsKey(field.getName())) {
        String fieldName = field.getName();
        Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() :
          field.getSchema();

        if (!nonNullableSchema.getType().equals(Schema.Type.BYTES) &&
          !nonNullableSchema.getType().equals(Schema.Type.STRING) || nonNullableSchema.getLogicalType() != null) {
          collector.addFailure(String.format("Input field '%s' is of unsupported type '%s'.",
                                             field.getName(), nonNullableSchema.getDisplayName()),
                               "Supported input types are bytes and string.")
            .withInputSchemaField(field.getName())
            .withConfigElement(Config.NAME_DECODE, fieldName + Config.SEPARATOR + decodeMap.get(fieldName));
        }
      }
    }
  }

  /**
   * Defines decoding types supported.
   */
  private enum DecoderType {
    BASE64("BASE64"),
    BASE32("BASE32"),
    STRING_BASE32("STRING_BASE32"),
    STRING_BASE64("STRING_BASE64"),
    HEX("HEX"),
    NONE("NONE");

    private String type;

    DecoderType(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  /**
   * Decoder Plugin config.
   */
  public static class Config extends PluginConfig {
    private static final String NAME_DECODE = "decode";
    private static final String NAME_SCHEMA = "schema";
    private static final String SEPARATOR = ":";

    @Name(NAME_DECODE)
    @Description("Specify the field and decode type combination. " +
      "Format is <field>:<decode-type>[,<field>:<decode-type>]*")
    private final String decode;

    @Name(NAME_SCHEMA)
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String decode, String schema) {
      this.decode = decode;
      this.schema = schema;
    }

    @Nullable
    private Schema getSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema : " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
      }
      throw collector.getOrThrowException();
    }
  }
}
