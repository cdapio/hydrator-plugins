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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Class to generate text based features using Hashing TF.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(HashingTFFeatureGenerator.PLUGIN_NAME)
@Description("Trains a regression model based upon a particular label and features of a record.")

public class HashingTFFeatureGenerator extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "HashingTFFeatureGenerator";
  private HashingTFConfig config;
  private StructType schema;
  private Schema outputSchema;
  private HashingTF hashingTF;
  private List<String> fieldList = new ArrayList<>();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    hashingTF = new HashingTF().setNumFeatures(config.numFeatures);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {
    if (input == null) {
      return context.getSparkContext().emptyRDD();
    }
    List<Schema.Field> fields = input.first().getSchema().getFields();
    if (fieldList.isEmpty()) {
      for (Schema.Field field : fields) {
        fieldList.add(field.getName());
      }
      fieldList.addAll(config.getFeatureListMapping().values());
    }
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    List<StructField> structField = SparkUtils.getStructFieldList(fields, config.getFeatureListMapping().keySet());
    schema = new StructType(structField.toArray(new StructField[structField.size()]));

    JavaRDD<Row> rowRDD = SparkUtils.convertJavaRddStructuredRecordToJavaRddRows(input, schema);
    DataFrame documentDF = sqlContext.createDataFrame(rowRDD, schema);
    DataFrame result = null;
    for (Map.Entry<String, String> entry : config.getFeatureListMapping().entrySet()) {
      result = hashingTF.setInputCol(entry.getKey() + "-transformed").setOutputCol(entry.getValue())
        .transform(documentDF);
      documentDF = result;
    }
    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(input.first().getSchema());
    List<Column> requiredFields = new ArrayList<>();
    for (String field : fieldList) {
      requiredFields.add(new Column(field));
    }
    JavaRDD<Row> transformedRDD =
      javaSparkContext.parallelize(result.select(JavaConversions.asScalaBuffer(requiredFields).toSeq())
                                     .collectAsList());
    JavaRDD<StructuredRecord> output = transformedRDD.map(new Function<Row, StructuredRecord>() {
      @Override
      public StructuredRecord call(Row row) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        String[] fields = fieldList.toArray(new String[fieldList.size()]);
        for (int i = 0; i < fields.length; i++) {
          String fieldName = fields[i];
          if (config.getFeatureListMapping().values().contains(fields[i])) {
            SparseVector vector = ((Vector) row.get(i)).toSparse();
            builder.set(fieldName + "_size", vector.size());
            builder.set(fieldName + "_indices", new ArrayList<>(Arrays.asList(ArrayUtils.toObject(vector.indices()))));
            builder.set(fieldName + "_value", new ArrayList<>(Arrays.asList(ArrayUtils.toObject(vector.values()))));
          } else {
            Schema schema = outputSchema.getField(fieldName).getSchema();
            if (row.get(i) == null && !schema.isNullable()) {
              throw new IllegalArgumentException(String.format("Null value found for nun-nullable field %s.",
                                                               fieldName));
            }
            Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
            if (type.equals(Schema.Type.ARRAY)) {
              builder.set(fieldName, row.getList(i));
            } else if (type.equals(Schema.Type.MAP)) {
              builder.set(fieldName, row.getJavaMap(i));
            } else {
              builder.set(fieldName, row.get(i));
            }
          }
        }
        return builder.build();
      }
    });
    return output;
  }

  /**
   * Configuration for the HashingTFFeatureGenerator.
   */
  public static class HashingTFConfig extends PluginConfig {
    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer. The default value " +
      "if none is provided will be 100.")
    private final Integer numFeatures;

    @Description("A comma-separated list of the input fields to map to the transformed output fields. The key " +
      "specifies the name of the field to generate feature vector from, with its corresponding value specifying the " +
      "output columns(size, indices and value) to emit the sparse vector.")
    private final String outputColumnMapping;

    public HashingTFConfig(@Nullable Integer numFeatures, String outputColumnMapping) {
      this.numFeatures = numFeatures;
      this.outputColumnMapping = outputColumnMapping;
    }

    private Map<String, String> getFeatureListMapping() {
      try {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator(":").split(outputColumnMapping);
        return map;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid categorical feature mapping. %s. Please make sure it is in the format " +
                          "'feature':'cardinality'.", e.getMessage()), e);
      }
    }

    private Schema getOutputSchema(Schema inputSchema) {
      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      for (Map.Entry<String, String> entry : getFeatureListMapping().entrySet()) {
        fields.add(Schema.Field.of(entry.getValue() + "_size", Schema.of(Schema.Type.INT)));
        fields.add(Schema.Field.of(entry.getValue() + "_indices", Schema.arrayOf(Schema.of(Schema.Type.INT))));
        fields.add(Schema.Field.of(entry.getValue() + "_value", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
      }
      return Schema.recordOf("record", fields);
    }
  }
}
