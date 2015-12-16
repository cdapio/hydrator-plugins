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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.format.StructuredRecordStringConverter;

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
    try {
      outSchema = Schema.parseJson(config.schema);
      type = outSchema.getFields().get(0).getSchema().getType();
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    try {
      Schema out = Schema.parseJson(config.schema);
      List<Schema.Field> fields = out.getFields();
      
      // For this plugin, the output schema needs to have only one field and it should 
      // be of type BYTES or STRING.
      if (fields.size() > 1) {
        throw new IllegalArgumentException("Only one output field should exist for this transform and it should " +
                                             "ne of type String");  
      }
      
      // Check to make sure the field type specified in the output is only of type
      // STRING or BYTES.
      if (fields.get(0).getSchema().getType() != Schema.Type.STRING &&
        fields.get(0).getSchema().getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Output field name should be of type String. Please change type to " +
                                             "String or Bytes");
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(out);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
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
    @Name("schema")
    @Description("Output schema")
    private String schema;


    public Config(String schema) {
      this.schema = schema;
    }

  }
}
