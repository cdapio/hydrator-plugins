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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.io.IOException;
import java.util.List;

/**
 * JSON Formatter formats {@link StructuredRecord} into JSON Object.
 */
@Plugin(type = "transform")
@Name("JSONFormatter")
@Description("Writes JSON Object formatted records from the Structured record.")
public final class JSONFormatter extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  
  // Output schema specified during configuration.
  private Schema outSchema;
  
  // Type of the field in the output schema where the JSON would be written to. 
  // Allows only BYTE or STRING fields. 
  private Schema.Type type;

  // Required only for testing.
  public JSONFormatter(Config config) {
    this.config = config;
  }

  /**
   * Initializes the plugin by parsing the schema JSON. 
   *  
   * @param context Context of transformation.
   * @throws Exception If Schema JSON is invalid. 
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    // Get failure collector for updated validation API
    FailureCollector collector = getContext().getFailureCollector();
    try {
      outSchema = Schema.parseJson(config.schema);
      type = outSchema.getFields().get(0).getSchema().getType();
    } catch (IOException e) {
      collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.")
        .withConfigProperty(Config.SCHEMA);
      throw collector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    if (!TransformLineageRecorderUtils.getFields(context.getOutputSchema()).isEmpty()) {
      context.record(TransformLineageRecorderUtils.generateManyToOne(
        TransformLineageRecorderUtils.getFields(context.getInputSchema()),
        TransformLineageRecorderUtils.getFields(context.getOutputSchema()).get(0),
        "jsonFormat", "Formatted data as a JSON string."));
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Get failure collector for updated validation API
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    try {
      Schema out = Schema.parseJson(config.schema);
      List<Schema.Field> fields = out.getFields();
      
      // For this plugin, the output schema needs to have only one field and it should 
      // be of type BYTES or STRING.
      if (fields.size() > 1) {
        collector.addFailure("Output schema may only have one field.", null);
      }
      
      // Check to make sure the field type specified in the output is only of type
      // STRING or BYTES.
      Schema.Field field = fields.get(0);
      Schema.Type type = field.getSchema().getType();
      if (fields.get(0).getSchema().getType() != Schema.Type.STRING &&
        fields.get(0).getSchema().getType() != Schema.Type.BYTES) {
        collector.addFailure(String.format("Invalid output field type '%s' for field '%s'.",
                                           field.getName(), type.toString().toLowerCase()),
                             "Output field type must be of type string or bytes.")
          .withOutputSchemaField(field.getName());
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(out);
    } catch (IOException e) {
      collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.")
        .withConfigProperty(Config.SCHEMA);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder record = StructuredRecord.builder(outSchema);
    
    // Convert the structured record to JSON.
    String outputRecord = StructuredRecordStringConverter.toJsonString(input);
    
    // Depending on the output field type emit it as string or bytes.
    if (type == Schema.Type.BYTES) {
      record.set(outSchema.getFields().get(0).getName(), outputRecord.getBytes());
    } else if (type == Schema.Type.STRING) {
      record.set(outSchema.getFields().get(0).getName(), outputRecord);
    }
    emitter.emit(record.build());
  }

  /**
   * JSON Writer Plugin Configuration.
   */
  public static class Config extends PluginConfig {
    public static final String SCHEMA = "schema";

    @Name("schema")
    @Description("Output schema")
    private String schema;


    public Config(String schema) {
      this.schema = schema;
    }

  }
}
