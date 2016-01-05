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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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

  private void parseConfiguration(String config) throws IllegalArgumentException {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(":");

      // If format is not right, then we throw an exception.
      if (params.length < 2) {
        throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                                             "Format should be <fieldname>:<decompressor-type>");
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      DecompressorType cType = DecompressorType.valueOf(type);

      if (deCompMap.containsKey(field)) {
        throw new IllegalArgumentException("Field " + field + " already has decompressor set. Check the mapping.");
      } else {
        deCompMap.put(field, cType);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.decompressor);

    // Check if schema specified is a valid schema or no.
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outputSchema.getFields();
      for (Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format. " +
                                           e.getMessage());
    }

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (outSchemaMap.containsKey(field.getName()) &&
          deCompMap.containsKey(field.getName()) && deCompMap.get(field.getName()) != DecompressorType.NONE &&
          !Schema.Type.BYTES.equals(field.getSchema().getType())) {
          throw new IllegalArgumentException(
            String.format("Input field  %s should be of type bytes or string. It is currently of type %s",
                          field.getName(), field.getSchema().getType().toString()));
        }
      }
    }

    for (Map.Entry<String, DecompressorType> entry : deCompMap.entrySet()) {
      if (!outSchemaMap.containsKey(entry.getKey())) {
        throw new IllegalArgumentException("Field '" + entry.getKey() + "' specified to be decompresed is not " +
                                             "present in the output schema. Please add field '" + entry.getKey() + "'" +
                                             "to output schema or remove it from decompress");
      }
      if (outSchemaMap.get(entry.getKey()) != Schema.Type.BYTES &&
        outSchemaMap.get(entry.getKey()) != Schema.Type.STRING) {
        throw new IllegalArgumentException("Field '" + entry.getKey() + "' should be of type BYTES or STRING in " +
                                             "output schema.");
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.decompressor);
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

  /**
   * Decompressor Plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("decompressor")
    @Description("Specify the field and decompression type combination. " +
      "Format is <field>:<decompressor-type>[,<field>:<decompressor-type>]*")
    private final String decompressor;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String decompressor, String schema) {
      this.decompressor = decompressor;
      this.schema = schema;
    }
  }
}
