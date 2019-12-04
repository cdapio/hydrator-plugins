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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.ws.rs.Path;

/**
 * Compresses the configured fields using the algorithms specified.
 */
@Plugin(type = "transform")
@Name("Compressor")
@Description("Compresses configured fields using the algorithms specified.")
public final class Compressor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Compressor.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Output Field name to type map
  private Map<String, Schema.Type> outSchemaMap = Maps.newHashMap();

  private final Map<String, CompressorType> compMap = Maps.newTreeMap();

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Compressor(Config config) {
    this.config = config;
  }

  private void parseConfiguration(String config) throws IllegalArgumentException {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(":");

      // If format is not right, then we throw an exception.
      if (params.length < 2) {
        throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
            "Format should be <fieldname>:<compressor-type>");
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      CompressorType cType = CompressorType.valueOf(type);

      if (compMap.containsKey(field)) {
        throw new IllegalArgumentException("Field " + field + " already has compressor set. Check the mapping.");
      } else {
        compMap.put(field, cType);
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.compressor);
    try {
      outSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outSchema.getFields();
      for (Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }

      for (String field : compMap.keySet()) {
        if (compMap.containsKey(field)) {
          Schema.Type type = outSchemaMap.get(field);
          if (type != Schema.Type.BYTES) {
            throw new IllegalArgumentException("Field '" + field + "' is not of type BYTES. It's currently" +
                "of type '" + type.toString() + "'.");
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.compressor);

    // Check if schema specified is a valid schema or no. 
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outputSchema.getFields();
      for (Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (outSchemaMap.containsKey(field.getName()) &&
            compMap.containsKey(field.getName()) && compMap.get(field.getName()) != CompressorType.NONE &&
            !Schema.Type.BYTES.equals(field.getSchema().getType()) &&
            !Schema.Type.STRING.equals(field.getSchema().getType())) {
          if (field.getSchema().getType().name().equalsIgnoreCase("UNION")) {
            throw new IllegalArgumentException(
                String.format("Input compressor field  %s must be of non-nullable type . It is currently of type %s",
                    field.getName(), "nullable"));
          } else {
            throw new IllegalArgumentException(
                String.format("Input field  %s must be of type bytes or string. It is currently of type %s",
                    field.getName(), field.getSchema().getType().toString()));
          }
        }
      }
    }
  }


  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);

    Schema inSchema = in.getSchema();
    List<Field> inFields = inSchema.getFields();

    // Iterate through input fields. Check if field name is present 
    // in the fields that need to be compressed, if it's not then write
    // to output as it is. 
    for (Field field : inFields) {
      String name = field.getName();

      // Check if output schema also have the same field name. If it's not 
      // then continue.
      if (!outSchemaMap.containsKey(name)) {
        continue;
      }

      Schema.Type outFieldType = outSchemaMap.get(name);

      // Check if the input field name is configured to be compressed. If the field is not
      // present or is defined as none, then pass through the field as is. 
      if (!compMap.containsKey(name) || compMap.get(name) == CompressorType.NONE) {
        builder.set(name, in.get(name));
      } else {
        // Now, the input field could be of type String or byte[], so transform everything
        // to byte[] 
        byte[] obj = new byte[0];
        if (field.getSchema().getType() == Schema.Type.BYTES) {
          obj = in.get(name);
        } else if (field.getSchema().getType() == Schema.Type.STRING) {
          obj = Bytes.toBytes((String) in.get(name));
        }

        // Now, based on the compressor type configured for the field - compress the byte[] of the
        // value.
        byte[] outValue = new byte[0];
        CompressorType type = compMap.get(name);
        if (type == CompressorType.SNAPPY) {
          outValue = Snappy.compress(obj);
        } else if (type == CompressorType.ZIP) {
          outValue = zip(obj);
        } else if (type == CompressorType.GZIP) {
          outValue = gzip(obj);
        }

        // Depending on the output field type, either convert it to 
        // Bytes or to String. 
        if (outFieldType == Schema.Type.BYTES) {
          if (outValue != null) {
            builder.set(name, outValue);
          }
        } else {
          LOG.warn("Output field '" + name + "' is not of BYTES. In order to emit compressed data, you should set " +
              "it to type BYTES.");
        }
      }
    }
    emitter.emit(builder.build());
  }

  private static byte[] gzip(byte[] input) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = null;
    try {
      gzip = new GZIPOutputStream(out);
      gzip.write(input, 0, input.length);
    } catch (IOException e) {
      // These are all in memory operations, so this should not happen. 
      // But, if it happens then we just return null. Logging anything 
      // here can be noise.
      return null;
    } finally {
      if (gzip != null) {
        try {
          gzip.close();
        } catch (IOException e) {
          return null;
        }
      }
    }

    return out.toByteArray();
  }

  private byte[] zip(byte[] input) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(out);
    try {
      zos.setLevel(9);
      zos.putNextEntry(new ZipEntry("c"));
      zos.write(input, 0, input.length);
      zos.finish();
    } catch (IOException e) {
      // These are all in memory operations, so this should not happen. 
      // But, if it happens then we just return null. Logging anything 
      // here can be noise. 
      return null;
    } finally {
      try {
        if (zos != null) {
          zos.close();
        }
      } catch (IOException e) {
        return null;
      }
    }

    return out.toByteArray();
  }

  /**
   * Enum specifying the compressor type.
   */
  private enum CompressorType {
    SNAPPY("SNAPPY"),
    ZIP("ZIP"),
    GZIP("GZIP"),
    NONE("NONE");

    private String type;

    CompressorType(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  /**
   * END
   */

  public static class GetSchemaRequest extends Config {
    private Schema inputSchema;

    public GetSchemaRequest(String compressor, String schema) {
      super(compressor, schema);
    }
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return request.getOutputSchema(request.inputSchema);
  }

  /**
   * Plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("compressor")
    @Description("Specify the field and compression type combination. " +
        "Format is <field>:<compressor-type>[,<field>:<compressor-type>]*")
    private final String compressor;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String compressor, String schema) {
      this.compressor = compressor;
      this.schema = schema;
    }

    public Schema getOutputSchema(Schema inputSchema) {
      try {
        List<Schema.Field> fields = new ArrayList<>();
        Map<String, CompressorType> compressorTypeMap = parseConfiguration(compressor);
        if (inputSchema != null && inputSchema.getFields() != null) {
          for (Schema.Field field : inputSchema.getFields()) {
            if (compressorTypeMap.containsKey(field.getName())) {
              fields.add(Schema.Field.of(field.getName(), Schema.of(Schema.Type.BYTES)));
            } else {
              fields.add(field);
            }
          }
        }
        return Schema.recordOf("outputSchema", fields);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to create output schema ==> " + e.getMessage(), e);
      }
    }

    private Map<String, CompressorType> parseConfiguration(String config) throws IllegalArgumentException {
      Map<String, CompressorType> compMap = Maps.newTreeMap();
      String[] mappings = config.split(",");
      for (String mapping : mappings) {
        String[] params = mapping.split(":");

        // If format is not right, then we throw an exception.
        if (params.length < 2) {
          throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
              "Format should be <fieldname>:<compressor-type>");
        }

        String field = params[0];
        String type = params[1].toUpperCase();
        CompressorType cType = CompressorType.valueOf(type);

        if (compMap.containsKey(field)) {
          throw new IllegalArgumentException("Field " + field + " already has compressor set. Check the mapping.");
        } else {
          compMap.put(field, cType);
        }
      }
      return compMap;
    }
  }

}
