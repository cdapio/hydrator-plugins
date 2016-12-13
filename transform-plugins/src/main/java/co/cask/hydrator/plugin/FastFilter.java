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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Quickly filters records based on a set criteria.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FastFilter")
@Description("Only allows records through that pass the specified criteria.")
public final class FastFilter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(FastFilter.class);
  private final Config config;
  private Pattern regexPattern;

  // Required only for testing.
  public FastFilter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    // Just checking that the operator is valid
    shouldAllowThrough("", config.operator, "");
    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if (config.operator.contains("regex")) {
      regexPattern = Pattern.compile(config.criteria);
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    if (in.get(config.sourceField) != null) {
      String sourceContent = String.valueOf(in.get(config.sourceField)).trim();
      if (config.shouldIgnoreCase) {
        sourceContent = sourceContent.toLowerCase();
      }
      if (shouldAllowThrough(sourceContent, config.operator, config.criteria)) {
        List<Schema.Field> fields = in.getSchema().getFields();
        StructuredRecord.Builder builder = StructuredRecord.builder(in.getSchema());
        for (Schema.Field field : fields) {
          String name = field.getName();
          builder.set(name, in.get(name));
        }
        emitter.emit(builder.build());
      }
    }
  }

  private boolean shouldAllowThrough(String sourceContent, String operator, String criteria) {
    switch (operator) {
      case "=":
        return sourceContent.equals(criteria);
      case "!=":
        return !sourceContent.equals(criteria);
      case ">":
        return (sourceContent.compareTo(criteria) > 0);
      case ">=":
        return (sourceContent.compareTo(criteria) >= 0);
      case "<":
        return (sourceContent.compareTo(criteria) < 0);
      case "<=":
        return (sourceContent.compareTo(criteria) <= 0);
      case "contains":
        return sourceContent.contains(criteria);
      case "does not contain":
        return !sourceContent.contains(criteria);
      case "starts with":
        return sourceContent.startsWith(criteria);
      case "ends with":
        return sourceContent.endsWith(criteria);
      case "doesn't start with":
        return !sourceContent.startsWith(criteria);
      case "doesn't end with":
        return !sourceContent.endsWith(criteria);
      case "matches regex":
        return regexPattern.matcher(sourceContent).find();
      case "does not match regex":
        return !regexPattern.matcher(sourceContent).find();
      default:
        throw new IllegalArgumentException("Invalid operator: " + operator);
    }
  }

  /**
   * Fast filter plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("sourceField")
    @Description("Specifies the input field to use in the filter.")
    @Macro
    private final String sourceField;

    @Name("operator")
    @Description("The operator to be used for the filter.")
    private final String operator;

    @Name("criteria")
    @Description("The criteria to be used for the filter.")
    @Macro
    private final String criteria;

    @Name("shouldIgnoreCase")
    @Description("Set to true to ignore the case of the field when matching.")
    private final Boolean shouldIgnoreCase;


    public Config(String sourceField, String operator, String criteria, boolean shouldIgnoreCase) {
      this.sourceField = sourceField;
      this.operator = operator;
      this.criteria = criteria;
      this.shouldIgnoreCase = shouldIgnoreCase;
    }

    public void validate() {
      if (operator.contains("regex")) {
        Pattern.compile(criteria);
      }
    }
  }
}
