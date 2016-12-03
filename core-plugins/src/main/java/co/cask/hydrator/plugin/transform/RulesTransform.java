/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.hydrator.common.KeyValueListParser;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created for hackathon on 12/2/16.
 */
@Plugin(type = "transform")
@Name("Rules")
public class RulesTransform extends Transform<StructuredRecord, StructuredRecord> {

  private final RulesTranformConfig rulesTranformConfig;
  private static Map<String, String> ruleConfigs = new HashMap<>();

  public RulesTransform(RulesTranformConfig rulesTranformConfig) {
    this.rulesTranformConfig = rulesTranformConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema outputSchema = null;
    if (pipelineConfigurer.getStageConfigurer().getInputSchema() != null) {
      //validate the input schema and get the output schema for it
      outputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    ruleConfigs.put("startsWith", null);
    ruleConfigs.put("endsWith", null);
    ruleConfigs.put("contains", null);
    ruleConfigs.put("regex", null);
    ruleConfigs.put("length", null);
    ruleConfigs.put("isNumber", null);
    ruleConfigs.put("isAlphaNumeric", null);
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    Set<String> ruleTypes = ruleConfigs.keySet();
    if (!Strings.isNullOrEmpty(rulesTranformConfig.rules)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(rulesTranformConfig.rules)) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        if (ruleTypes.contains(key)) {
          ruleConfigs.put(key, val);
        } else {
          throw new IllegalArgumentException(String.format("%s is not a valid ruleType", key));
        }
      }
    }
  }

  @Override
  public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) throws Exception {
    Schema inputSchema = valueIn.getSchema();
    String value = valueIn.get(rulesTranformConfig.field);
    if (ruleConfigs.get("isNumber") != null && ruleConfigs.get("isNumber").equalsIgnoreCase("true")  &&
      !StringUtils.isNumericSpace(value)) {
      String error = String.format("String %s is not a number", value);
      emitter.emitError(new InvalidEntry<>(1, error, valueIn));
      return;
    }
    if (ruleConfigs.get("isAlphaNumeric") != null) {
      if (!StringUtils.isAlphanumericSpace(value)) {
        String error = String.format("String %s is not alphanumeric", value);
        emitter.emitError(new InvalidEntry<>(1, error, valueIn));
        return;
      }
    }
    if (ruleConfigs.get("startsWith") != null) {
      if (!value.startsWith(ruleConfigs.get("startsWith"))) {
        String error = String.format("String %s does not starts with %s", value, ruleConfigs.get("startsWith"));
        emitter.emitError(new InvalidEntry<>(1, error, valueIn));
        return;
      }
    }

    if (ruleConfigs.get("endsWith") != null) {
      if (!value.endsWith(ruleConfigs.get("endsWith"))) {
        String error = String.format("String %s does not ends with %s", value, ruleConfigs.get("endsWith"));
        emitter.emitError(new InvalidEntry<>(1, error, valueIn));
        return;
      }
    }

    if (ruleConfigs.get("contains") != null) {
      if (!value.contains(ruleConfigs.get("contains"))) {
        String error = String.format("String %s does not contain %s", value, ruleConfigs.get("contains"));
        emitter.emitError(new InvalidEntry<>(1, error, valueIn));
        return;
      }
    }
    if (ruleConfigs.get("length") != null) {
      if (value.length() != Integer.parseInt(ruleConfigs.get("length"))) {
        String error = String.format("String %s is not of length %s", value, ruleConfigs.get("length"));
        emitter.emitError(new InvalidEntry<>(1, error, valueIn));
      }
    }
    emitter.emit(valueIn);
  }

  /**
   *
   */
  public static class RulesTranformConfig extends PluginConfig {

    @Name("field")
    @Description("Specify the field that should be used to apply rules to.")
    private final String field;

    @Name("rules")
    @Description("Specify the rules")
    private final String rules;

    public RulesTranformConfig(String field, String rules) {
      this.field = field;
      this.rules = rules;
    }
  }
}
