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
 * Encodes the input fields as BASE64, BASE32 or HEX.
 */
@Plugin(type = "transform")
@Name("Encoder")
@Description("Encodes the input field(s) using Base64, Base32 or Hex")
public final class Encoder extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Mapping of input field to encoder type. 
  private final Map<String, EncodeType> encodeMap = new TreeMap<>();

  // Encoder handlers.
  private final Base64 base64Encoder = new Base64();
  private final Base32 base32Encoder = new Base32();
  private final Hex hexEncoder = new Hex();

  // Output Field name to type map
  private final Map<String, Schema.Type> outSchemaMap = new HashMap<>();

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Encoder(Config config) {
    this.config = config;
  }

  private void parseConfiguration(String config) throws IllegalArgumentException {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(":");

      // If format is not right, then we throw an exception.
      if (params.length < 2) {
        throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                                             "Format should be <fieldname>:<encoder-type>");
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      EncodeType eType = EncodeType.valueOf(type);

      if (encodeMap.containsKey(field)) {
        throw new IllegalArgumentException("Field " + field + " already has encoder set. Check the mapping.");
      } else {
        encodeMap.put(field, eType);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.encode);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    // for the fields in input schema, if they are to be encoded (if present in encodeMap)
    // make sure their type is either String or Bytes and throw exception otherwise
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (encodeMap.containsKey(field.getName())) {
          if (!field.getSchema().getType().equals(Schema.Type.BYTES) &&
            !field.getSchema().getType().equals(Schema.Type.STRING)) {
            throw new IllegalArgumentException(
              String.format("Input field  %s should be of type bytes or string. It is currently of type %s",
                            field.getName(), field.getSchema().getType().toString()));
          }
        }
      }
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
    parseConfiguration(config.encode);
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
    // in the fields that need to be encoded, if it's not then write 
    // to output as it is. 
    for (Field field : inFields) {
      String name = field.getName();

      // Check if output schema also have the same field name. If it's not 
      // then throw an exception. 
      if (!outSchemaMap.containsKey(name)) {
        continue;
      }

      Schema.Type outFieldType = outSchemaMap.get(name);

      // Check if the input field name is configured to be encoded. If the field is not 
      // present or is defined as none, then pass through the field as is. 
      if (!encodeMap.containsKey(name) || encodeMap.get(name) == EncodeType.NONE) {
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

        // Now, based on the encode type configured for the field - encode the byte[] of the 
        // value.
        byte[] outValue = new byte[0];
        EncodeType type = encodeMap.get(name);
        if (type == EncodeType.STRING_BASE32) {
          outValue = base32Encoder.encodeAsString(obj).getBytes();
        } else if (type == EncodeType.BASE32) {
          outValue = base32Encoder.encode(obj);
        } else if (type == EncodeType.STRING_BASE64) {
          outValue = base64Encoder.encodeAsString(obj).getBytes();
        } else if (type == EncodeType.BASE64) {
          outValue = base64Encoder.encode(obj);
        } else if (type == EncodeType.HEX) {
          outValue = hexEncoder.encode(obj);
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

  /**
   * Defines encoding types supported.  
   */
  private enum EncodeType {
    STRING_BASE64("STRING_BASE64"),
    STRING_BASE32("STRING_BASE32"),
    BASE64("BASE64"),
    BASE32("BASE32"),
    HEX("HEX"),
    NONE("NONE");

    private String type;

    EncodeType(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  /**
   * Encoder Plugin config.
   */
  public static class Config extends PluginConfig {
    @Name("encode")
    @Description("Specify the field and encode type combination. " +
      "Format is <field>:<encode-type>[,<field>:<encode-type>]*")
    private final String encode;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String encode, String schema) {
      this.encode = encode;
      this.schema = schema;
    }
  }
}
