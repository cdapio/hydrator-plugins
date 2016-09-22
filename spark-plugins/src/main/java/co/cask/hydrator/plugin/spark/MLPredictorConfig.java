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

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import joptsimple.internal.Strings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Config class for Classifiers/Predictors. Contains common config properties and validation methods to be used
 * in Classifiers/Predictors.
 */
public class MLPredictorConfig extends PluginConfig {

  @Description("The name of the FileSet to load the model from.")
  protected String fileSetName;

  @Description("Path of the FileSet to load the model from.")
  protected String path;

  @Nullable
  @Description("A comma-separated sequence of fields that needs to be used for classification/prediction.")
  protected String featureFieldsToInclude;

  @Nullable
  @Description("A comma-separated sequence of fields that needs to be excluded from being used " +
    "in classification/prediction.")
  protected String featureFieldsToExclude;

  @Description("The field on which to set the prediction. It will be of type double.")
  protected String predictionField;

  public MLPredictorConfig() {
  }

  public MLPredictorConfig(String fileSetName, String path, @Nullable String featureFieldsToInclude,
                         @Nullable String featureFieldsToExclude, String predictionField) {
    this.fileSetName = fileSetName;
    this.path = path;
    this.featureFieldsToInclude = featureFieldsToInclude;
    this.featureFieldsToExclude = featureFieldsToExclude;
    this.predictionField = predictionField;
  }

  /**
   * Validate the config parameters for the spark sink class.
   *
   * @param inputSchema       schema of the received record.
   * @param featuresToInclude features to be used for training.
   * @param featuresToExclude features to be excluded when training.
   * @param predictionField   field containing the label values.
   */
  public static void validateConfigParameters(Schema inputSchema, @Nullable String featuresToInclude,
                                              @Nullable String featuresToExclude, String predictionField,
                                              @Nullable String cardinalityMapping) {
    if (!Strings.isNullOrEmpty(featuresToExclude) && !Strings.isNullOrEmpty(featuresToInclude)) {
      throw new IllegalArgumentException("Cannot specify values for both featuresToInclude and featuresToExclude. " +
                                           "Please specify fields for one.");
    }

    Schema.Field prediction = inputSchema.getField(predictionField);
    if (prediction != null) {
      throw new IllegalArgumentException(String.format("Prediction field %s should not be present in the input schema.",
                                                       predictionField));
    }

    getCategoricalFeatureInfo(inputSchema, featuresToInclude, featuresToExclude, predictionField, cardinalityMapping);
  }

  /**
   * Get the feature list of the features that have to be used for training/prediction depending on the
   * featuresToInclude or featuresToInclude list.
   *
   * @param inputSchema       schema of the received record.
   * @param featuresToInclude features to be used for training/prediction.
   * @param featuresToExclude features to be excluded when training/predicting.
   * @param predictionField   field containing the label values.
   * @return feature list to be used for training/prediction.
   */
  static Map<String, Integer> getFeatureList(Schema inputSchema, @Nullable String featuresToInclude,
                                             @Nullable String featuresToExclude, String predictionField) {
    if (!Strings.isNullOrEmpty(featuresToExclude) && !Strings.isNullOrEmpty(featuresToInclude)) {
      throw new IllegalArgumentException("Cannot specify values for both featuresToInclude and featuresToExclude. " +
                                           "Please specify fields for one.");
    }
    Map<String, Integer> fields = new HashMap<>();
    if (!Strings.isNullOrEmpty(featuresToInclude)) {
      Iterable<String> tokens = Splitter.on(',').trimResults().split(featuresToInclude);
      String[] features = Iterables.toArray(tokens, String.class);
      for (int i = 0; i < features.length; i++) {
        String field = features[i];
        Schema.Field inputField = inputSchema.getField(field);
        if (!field.equals(predictionField) && inputField != null) {
          fields.put(field, i);
        }
      }
      return fields;
    }

    Set<String> excludeFeatures = new HashSet<>();
    if (!Strings.isNullOrEmpty(featuresToExclude)) {
      excludeFeatures.addAll(Lists.newArrayList(Splitter.on(',').trimResults().split(featuresToExclude)));
    }
    Object[] inputSchemaFields = inputSchema.getFields().toArray();
    for (int i = 0; i < inputSchemaFields.length; i++) {
      String field = ((Schema.Field) inputSchemaFields[i]).getName();
      if (!field.equals(predictionField) && !excludeFeatures.contains(field)) {
        fields.put(field, i);
      }
    }
    return fields;
  }

  /**
   * Get the feature to cardinality mapping provided by the user.
   *
   * @param inputSchema        schema of the received record.
   * @param featuresToInclude  features to be used for training/prediction.
   * @param featuresToExclude  features to be excluded when training/predicting.
   * @param predictionField         field containing the prediction values.
   * @param cardinalityMapping feature to cardinality mapping specified for categorical features.
   */
  static void getCategoricalFeatureInfo(Schema inputSchema,
                                        @Nullable String featuresToInclude,
                                        @Nullable String featuresToExclude,
                                        String predictionField,
                                        @Nullable String cardinalityMapping) {
    getFeatureList(inputSchema, featuresToInclude, featuresToExclude, predictionField);

    if (!Strings.isNullOrEmpty(cardinalityMapping)) {
      try {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator(":").split(cardinalityMapping);
        for (Map.Entry<String, String> field : map.entrySet()) {
          if (!Strings.isNullOrEmpty(field.getValue())) {
            try {
              Integer.parseInt(field.getValue());
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException(
                String.format("Invalid cardinality %s. Please specify valid integer for cardinality.",
                              field.getValue()));
            }
          }
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid categorical feature mapping. %s. Please make sure it is in the format " +
                          "'feature':'cardinality'.", e.getMessage()), e);
      }
    }
  }
}
