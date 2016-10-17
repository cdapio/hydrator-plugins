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
import co.cask.cdap.api.dataset.lib.FileSet;
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
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.twill.filesystem.Location;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class to generate text based features using SkipGram model (Spark's Word2Vec).
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(SkipGramFeatureGenerator.PLUGIN_NAME)
@Description("Generate text based features using SkipGram model (Spark's Word2Vec).")
public class SkipGramFeatureGenerator extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "SkipGramFeatureGenerator";
  private FeatureGeneratorConfig config;
  private Schema outputSchema;
  private Word2VecModel loadedModel;
  private StructType schema;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException(String.format("Failed to find model to use for Regression. Location does " +
                                                         "not exist: %s.", modelLocation));
    }
    // load the model from a file in the model fileset
    loadedModel = Word2VecModel.load(modelLocation.toURI().getPath());
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (input == null) {
      return context.getSparkContext().emptyRDD();
    }
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    Schema inputSchema = input.first().getSchema();
    List<StructField> structField = SparkUtils.getStructFieldList(inputSchema.getFields(),
                                                                  config.getFeatureListMapping().keySet());
    schema = new StructType(structField.toArray(new StructField[structField.size()]));
    JavaRDD<Row> rowRDD = SparkUtils.convertJavaRddStructuredRecordToJavaRddRows(input, schema);
    DataFrame documentDF = sqlContext.createDataFrame(rowRDD, schema);
    DataFrame result = null;

    for (Map.Entry<String, String> entry : config.getFeatureListMapping().entrySet()) {
      result = loadedModel.setInputCol(entry.getKey() + "-transformed").setOutputCol(entry.getValue())
        .transform(documentDF);
      documentDF = result;
    }

    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(inputSchema);

    List<Column> requiredFields = new ArrayList<>();
    for (Schema.Field field : outputSchema.getFields()) {
      requiredFields.add(new Column(field.getName()));
    }

    JavaRDD<Row> transformedRDD =
      javaSparkContext.parallelize(result.select(JavaConversions.asScalaBuffer(requiredFields).toSeq())
                                     .collectAsList());
    JavaRDD<StructuredRecord> output = transformedRDD.map(new Function<Row, StructuredRecord>() {
      @Override
      public StructuredRecord call(Row row) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        Schema.Field[] strings = outputSchema.getFields().toArray(new Schema.Field[outputSchema.getFields().size()]);
        for (int i = 0; i < strings.length; i++) {
          String fieldName = strings[i].getName();
          Schema schema = strings[i].getSchema();
          if (config.getFeatureListMapping().values().contains(strings[i].getName())) {
            double[] doubles = ((Vector) row.get(i)).toArray();
            builder.set(fieldName, new ArrayList<>(Arrays.asList(ArrayUtils.toObject(doubles))));
          } else {
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
   * Config class for SkipGramFeatureGenerator.
   */
  public static class FeatureGeneratorConfig extends PluginConfig {
    @Description("The name of the FileSet to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Description("List of the input fields to map to the transformed output fields. The key specifies the name of " +
      "the field to generate feature vector from, with its corresponding value the output column in which the vector " +
      "would be emitted.")
    private final String outputColumnMapping;

    private FeatureGeneratorConfig(String fileSetName, String path, String outputColumnMapping) {
      this.fileSetName = fileSetName;
      this.path = path;
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
        fields.add(Schema.Field.of(entry.getValue(), Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
      }
      return Schema.recordOf("record", fields);
    }
  }
}
