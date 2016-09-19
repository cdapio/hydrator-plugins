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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import joptsimple.internal.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spark plugin Utility class. Contains common code to be used in trainer and predictors.
 */
final class SparkUtils {

  private SparkUtils() {
  }

  /**
   * Validate the config parameters for the spark sink and spark compute classes.
   *
   * @param inputSchema       schema of the received record.
   * @param featuresToInclude features to be used for training/prediction.
   * @param featuresToExclude features to be excluded when training/predicting.
   * @param predictionField   field containing the prediction values.
   */
  static void validateConfigParameters(Schema inputSchema, @Nullable String featuresToInclude,
                                       @Nullable String featuresToExclude, String predictionField,
                                       @Nullable String cardinalityMapping) {
    if (!Strings.isNullOrEmpty(featuresToExclude) && !Strings.isNullOrEmpty(featuresToInclude)) {
      throw new IllegalArgumentException("Cannot specify values for both featuresToInclude and featuresToExclude. " +
                                           "Please specify fields for one.");
    }
    Map<String, Integer> fields = getFeatureList(inputSchema, featuresToInclude, featuresToExclude, predictionField);
    for (String field : fields.keySet()) {
      Schema.Field inputField = inputSchema.getField(field);
      Schema schema = inputField.getSchema();
      Schema.Type features = schema.isNullableSimple() ? schema.getNonNullable().getType() : schema.getType();
      if (!(features.equals(Schema.Type.INT) || features.equals(Schema.Type.LONG) ||
        features.equals(Schema.Type.FLOAT) || features.equals(Schema.Type.DOUBLE))) {
        throw new IllegalArgumentException(String.format("Features must be of type : int, double, float, long but " +
                                                           "was of type %s for field %s.", features, field));
      }
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
   * @param predictionField   field containing the prediction values.
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
   * @param labelField         field containing the prediction values.
   * @param cardinalityMapping feature to cardinality mapping specified for categorical features.
   * @return categoricalFeatureInfo for categorical features.
   */
  static Map<Integer, Integer> getCategoricalFeatureInfo(Schema inputSchema,
                                                         @Nullable String featuresToInclude,
                                                         @Nullable String featuresToExclude,
                                                         String labelField,
                                                         @Nullable String cardinalityMapping) {
    Map<String, Integer> featureList = getFeatureList(inputSchema, featuresToInclude, featuresToExclude, labelField);
    Map<Integer, Integer> outputFieldMappings = new HashMap<>();

    if (Strings.isNullOrEmpty(cardinalityMapping)) {
      return outputFieldMappings;
    }
    try {
      Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator(":").split(cardinalityMapping);
      for (Map.Entry<String, String> field : map.entrySet()) {
        String value = field.getValue();
        try {
          outputFieldMappings.put(featureList.get(field.getKey()), Integer.parseInt(value));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            String.format("Invalid cardinality %s. Please specify valid integer for cardinality.", value));
        }
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
        String.format("Invalid categorical feature mapping. %s. Please make sure it is in the format " +
                        "'feature':'cardinality'.", e.getMessage()), e);
    }
    return outputFieldMappings;
  }

  /**
   * Validate label field for trainer.
   *
   * @param inputSchema schema of the received record.
   * @param labelField  field from which to get the prediction.
   */
  static void validateLabelFieldForTrainer(Schema inputSchema, String labelField) {
    Schema.Field prediction = inputSchema.getField(labelField);
    if (prediction == null) {
      throw new IllegalArgumentException(String.format("Label field %s does not exists in the input schema.",
                                                       labelField));
    }
    Schema predictionSchema = prediction.getSchema();
    Schema.Type predictionFieldType = predictionSchema.isNullableSimple() ?
      predictionSchema.getNonNullable().getType() : predictionSchema.getType();
    if (predictionFieldType != Schema.Type.DOUBLE) {
      throw new IllegalArgumentException(String.format("Label field must be of type Double, but was %s.",
                                                       predictionFieldType));
    }
  }

  /**
   * Creates a builder based off the given record. The record will be cloned without the prediction field.
   */
  static StructuredRecord.Builder cloneRecord(StructuredRecord record, Schema outputSchema,
                                              String predictionField) {
    List<Schema.Field> fields = new ArrayList<>(outputSchema.getFields());
    fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.DOUBLE)));
    outputSchema = Schema.recordOf("records", fields);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      if (!predictionField.equals(field.getName())) {
        builder.set(field.getName(), record.get(field.getName()));
      }
    }
    return builder;
  }

  static Schema getOutputSchema(Schema inputSchema, String predictionField) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.DOUBLE)));
    return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);
  }
}
