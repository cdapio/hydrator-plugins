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

import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

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

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    parseConfiguration(config.compressor, context.getFailureCollector());

    // Initialize the required member maps and then: if a field is in input and output and comp, then
    // set it to transform; if it's in input and output but not comp, ignore it; if it's in input and
    // not output, drop it.
    List<String> inFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    List<String> outFields = TransformLineageRecorderUtils.getFields(context.getOutputSchema());
    List<String> identityFields = outFields.stream()
      .filter(field -> !compMap.containsKey(field) || compMap.get(field) == CompressorType.NONE)
      .collect(Collectors.toList());

    List<String> processedFields = new ArrayList<>(outFields);
    processedFields.removeAll(identityFields);
    List<String> droppedFields = new ArrayList<>(inFields);
    droppedFields.removeAll(outFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(processedFields, "compress",
      "Used the specified algorithm to compress the field."));
    output.addAll(TransformLineageRecorderUtils.generateDrops(droppedFields));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    parseConfiguration(config.compressor, collector);
    Schema outputSchema = getSchema(collector);

    List<Field> outFields = outputSchema.getFields();
    for (Field field : outFields) {
      outSchemaMap.put(field.getName(), field.getSchema().getType());
    }

    for (String fieldName : compMap.keySet()) {
      if (!outSchemaMap.containsKey(fieldName)) {
        collector.addFailure(String.format("Field '%s' must be in output schema.", fieldName), null)
          .withConfigElement(Config.NAME_COMPRESSOR, fieldName + Config.SEPARATOR + compMap.get(fieldName));
        continue;
      }

      Field field = outputSchema.getField(fieldName);
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();
      if (nonNullableSchema.getType() != Schema.Type.BYTES || nonNullableSchema.getLogicalType() != null) {
        collector.addFailure(String.format("Compress field '%s' is of invalid type '%s'.",
                                           field.getName(), nonNullableSchema.getDisplayName()),
                             "Ensure the compress field is of type bytes.")
          .withOutputSchemaField(field.getName())
          .withConfigElement(Config.NAME_COMPRESSOR, fieldName + Config.SEPARATOR + compMap.get(fieldName));
      }
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    validateInputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema(), collector);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.compressor, context.getFailureCollector());
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
            throw new IllegalArgumentException("Field '" + field + "' is not of type 'bytes'. It's " +
                                                 "of invalid type '" + type.toString() + "'.");
          }
        }
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

  private void parseConfiguration(String config, FailureCollector collector) {
    String[] mappings = config.split(",");
    for (String mapping : mappings) {
      String[] params = mapping.split(Config.SEPARATOR);

      if (params.length < 2) {
        collector.addFailure(String.format("Configuration '%s' is incorrectly formed.", mapping),
                             "Specify the configuration in the format <fieldname>:<compressor-type>.")
          .withConfigProperty(Config.NAME_COMPRESSOR);
        continue;
      }

      String field = params[0];
      String type = params[1].toUpperCase();
      CompressorType cType = CompressorType.valueOf(type);

      if (compMap.containsKey(field)) {
        collector.addFailure(String.format("Field '%s' already has compressor set.", field),
                             "Ensure different fields are provided.")
          .withConfigElement(Config.NAME_COMPRESSOR, mapping);
        continue;
      }

      compMap.put(field, cType);
    }
  }

  private void validateInputSchema(@Nullable Schema inputSchema, FailureCollector collector) {
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        String fieldName = field.getName();
        Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
          : field.getSchema();
        Schema.Type type = nonNullableSchema.getType();
        Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();

        if (outSchemaMap.containsKey(field.getName()) &&
          compMap.containsKey(fieldName) && compMap.get(fieldName) != CompressorType.NONE &&
          (!Schema.Type.BYTES.equals(type) && !Schema.Type.STRING.equals(type) || logicalType != null)) {
          collector.addFailure(String.format("Input field '%s' is of unsupported type '%s'.",
                                             field.getName(), nonNullableSchema.getDisplayName()),
                               "Supported input types are bytes and string.")
            .withInputSchemaField(field.getName())
            .withConfigElement(Config.NAME_COMPRESSOR, fieldName + Config.SEPARATOR + compMap.get(fieldName));
        }
      }
    }
  }

  private Schema getSchema(FailureCollector collector) {
    try {
      return Schema.parseJson(config.schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema : " + e.getMessage(), null).withConfigProperty(Config.NAME_SCHEMA);
    }
    throw collector.getOrThrowException();
  }

  /**
   * Plugin configuration.
   */
  public static class Config extends PluginConfig {
    private static final String NAME_COMPRESSOR = "compressor";
    private static final String NAME_SCHEMA = "schema";
    private static final String SEPARATOR = ":";

    @Name(NAME_COMPRESSOR)
    @Description("Specify the field and compression type combination. " +
      "Format is <field>:<compressor-type>[,<field>:<compressor-type>]*")
    private final String compressor;

    @Name(NAME_SCHEMA)
    @Description("Specifies the output schema")
    private final String schema;

    public Config(String compressor, String schema) {
      this.compressor = compressor;
      this.schema = schema;
    }
  }
}
