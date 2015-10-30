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
import com.algorithmia.Algorithmia;
import com.algorithmia.AlgorithmiaClient;
import com.algorithmia.TypeToken;
import com.algorithmia.algo.Algorithm;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Clones Input Record 'n' number of times into output.
 */
@Plugin(type = "transform")
@Name("AutoTagger")
@Description("Extracts keywords using a variation of LDA.")
public final class AutoTagger extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  // Output Schema Field name that is considered as Stream Event Header.
  private String tagFiledName;

  // Output Schema Field name that is considered as Stream Event Body.
  private String weightFieldName;
  
  // 
  private Schema outSchema;
  
  private Algorithm algorithm;

  // Required only for testing.
  public AutoTagger(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      outSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
    // Iterate through output schema and find out which one is header and which is body.
    tagFiledName = config.outFieldTag;
    weightFieldName = config.outFieldWeight;
    algorithm = Algorithmia.client(config.apiKey).algo("nlp/sentimentbyterm");
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);

    // Check if the output schema JSON is invalid
    try {
      Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the schema JSON");
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    String text = in.get(config.field);
    
    if(text == null || (text != null && text.isEmpty())) {
      return;
    }
    
    Map<String, Double> sentiments = algorithm.pipe(text).as(new TypeToken<Map<String, Double>>(){});
    Iterator it = sentiments.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
      
      for(Schema.Field field : in.getSchema().getFields()) {
        builder.set(field.getName(), in.get(field.getName()));
      }
      
      StructuredRecord record= builder.set(tagFiledName, pair.getKey())
        .set(weightFieldName, pair.getValue()).build();
      emitter.emit(record);
    }

  }

  /**
   * Clone rows plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("apiKey")
    @Description("Algorithmia API key.")
    private final String apiKey;
    
    @Name("field")
    @Description("Text field that has to be auto-tagged.")
    private final String field;
    
    @Name("outfieldtag")
    @Description("Name of the output field for tag")
    private final String outFieldTag;
    
    @Name("outfieldweight")
    @Description("Name of the output field for weight")
    private final String outFieldWeight;
    
    @Name("schema")
    @Description("Output Schema")
    private final String schema;
    
    public Config(String apiKey, String field, String outFieldTag, String outFieldWeight, String schema) {
      this.apiKey = apiKey;
      this.field = field;
      this.outFieldTag = outFieldTag;
      this.outFieldWeight = outFieldWeight;
      this.schema = schema;
    }
  }
}
