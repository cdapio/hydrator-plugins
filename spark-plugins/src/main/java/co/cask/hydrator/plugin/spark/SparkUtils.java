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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    Map<String, Integer> fields = new LinkedHashMap<>();

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

  private static DataType getStructDataTypes(Schema.Type type) {
    switch (type) {
      case STRING:
        return DataTypes.StringType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case BYTES:
        return DataTypes.ByteType;
      case NULL:
        return DataTypes.NullType;
      default:
        return DataTypes.StringType;
    }
  }

  private static StructField getStructField(Schema.Field field, Boolean transformField) {
    Schema schema = field.getSchema();
    Boolean isNullable = schema.isNullable();
    Schema.Type type = isNullable ? schema.getNonNullable().getType() : schema.getType();
    if (transformField) {
      if (type.equals(Schema.Type.ARRAY)) {
        type = schema.getComponentSchema().getType();
        return new StructField(field.getName() + "-transformed",
                               DataTypes.createArrayType(getStructDataTypes(type), isNullable), isNullable,
                               Metadata.empty());
      } else {
        return new StructField(field.getName() + "-transformed", new ArrayType(getStructDataTypes(type), true),
                               isNullable, Metadata.empty());
      }
    } else {
      if (type.equals(Schema.Type.ARRAY)) {
        type = schema.getComponentSchema().getType();
        return new StructField(field.getName(), DataTypes.createArrayType(getStructDataTypes(type), isNullable),
                               isNullable, Metadata.empty());
      } else if (type.equals(Schema.Type.MAP)) {
        return new StructField(field.getName(), DataTypes.createMapType(
          getStructDataTypes(field.getSchema().getMapSchema().getKey().getType()),
          getStructDataTypes(field.getSchema().getMapSchema().getValue().getType()), isNullable),
                               isNullable, Metadata.empty());
      } else {
        return new StructField(field.getName(), getStructDataTypes(type), isNullable, Metadata.empty());
      }
    }
  }

  static List<StructField> getStructFieldList(List<Schema.Field> fields, Set<String> inputCol) {
    List<StructField> structField = new ArrayList<>();
    for (Schema.Field field : fields) {
      if (inputCol.contains(field.getName())) {
        Schema schema = field.getSchema();
        Schema.Type type = schema.getType();
        if (Schema.Type.STRING.equals(type) || Schema.Type.BOOLEAN.equals(type) || (Schema.Type.ARRAY.equals(type) &&
          (Schema.Type.STRING.equals(schema.getComponentSchema().getType()) ||
            Schema.Type.BOOLEAN.equals(schema.getComponentSchema().getType())))) {
          structField.add(SparkUtils.getStructField(field, true));
        } else {
          throw new IllegalArgumentException(
            String.format("Unsupported data types for feature generation. Only string, boolean or array of type " +
                            "string are supported. But was %s for field %s", schema.getType(), field));
        }
      }
      structField.add(SparkUtils.getStructField(field, false));
    }
    return structField;
  }

  static JavaRDD<Row> convertJavaRddStructuredRecordToJavaRddRows(JavaRDD<StructuredRecord> input,
                                                                  final StructType schema) {
    return input.map(new Function<StructuredRecord, Row>() {
      @Override
      public Row call(StructuredRecord record) throws Exception {
        List<Object> fields = new ArrayList<>();
        for (String field : schema.fieldNames()) {
          if (field.contains("-transformed")) {
            field = field.split("-transformed")[0];
            if (Schema.Type.ARRAY.equals(record.getSchema().getField(field).getSchema().getType())) {
              fields.add(record.get(field));
            } else {
              fields.add(Arrays.asList(((String) record.get(field)).split(" ")));
            }
          } else {
            fields.add(record.get(field));
          }
        }
        return RowFactory.create(fields.toArray());
      }
    });
  }
}
