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
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Decodes the input fields as BASE64, BASE32 or HEX.
 * Please note that Encoder and Decoder might look the same right now, but in near future they will diverge.
 */
@Plugin(type = "transform")
@Name("Decoder")
@Description("Decodes the input field(s) using Base64, Base32, or Hex")
public final class Decoder extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Decoder.class);
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

  private void parseConfiguration(String config, FailureCollector collector) {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(":");

      // If format is not right, then we add a failure.
      if (params.length < 2) {
        collector.addFailure("Configuration " + mapping + " is incorrectly formed.",
            "Please specify the configuration in the format <fieldname>:<decoder-type>.")
            .withConfigProperty("decode");
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      DecoderType eType = DecoderType.valueOf(type);

      if (decodeMap.containsKey(field)) {
        collector.addFailure("Field " + field + " already has decoder set.",
            "Please check the mapping.").withConfigProperty("decode");
      } else {
        decodeMap.put(field, eType);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.decode, pipelineConfigurer.getStageConfigurer().getFailureCollector());

    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    validateInputSchema(configurer.getInputSchema(), collector);

    Schema outputSchema = getSchema(collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.decode, context.getFailureCollector());
    outSchema = getSchema(context.getFailureCollector());
    List<Field> outFields = outSchema.getFields();
    for (Field field : outFields) {
      outSchemaMap.put(field.getName(), field.getSchema().getType());
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

  private Schema getSchema(FailureCollector collector) {
    try {
      return Schema.parseJson(config.schema);
    } catch (IOException e) {
      collector.addFailure("Format of schema specified is invalid.", "Please check the format.")
      .withConfigProperty("schema");
    }
    throw collector.getOrThrowException();
  }

  private void validateInputSchema(Schema inputSchema, FailureCollector collector) {
    // for the fields in input schema, if they are to be decoded (if present in decodeMap)
    // make sure their type is either String or Bytes and throw exception otherwise
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (decodeMap.containsKey(field.getName())) {
          if (!field.getSchema().getType().equals(Schema.Type.BYTES) &&
              !field.getSchema().getType().equals(Schema.Type.STRING)) {
            collector.addFailure(String.format("Input field %s is of invalid type %s",
                field.getName(), field.getSchema().getType().toString()),
                "Please specify an input field of type bytes or string.")
                .withInputSchemaField(field.getName(), null);
          }
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
    @Name("decode")
    @Description("Specify the field and decode type combination. " +
      "Format is <field>:<decode-type>[,<field>:<decode-type>]*")
    private final String decode;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String decode, String schema) {
      this.decode = decode;
      this.schema = schema;
    }
  }
}
