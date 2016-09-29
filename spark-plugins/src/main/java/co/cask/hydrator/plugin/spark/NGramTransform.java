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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * NGramTransform - SparkCompute to transform input features into n-grams.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(NGramTransform.PLUGIN_NAME)
@Description("Used to transform input features into n-grams.")
public class NGramTransform extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "NGramTransform";
  private Config config;
  public Schema outputSchema;

  public NGramTransform(Config config) {
    this.config = config;
  }

  /**
   * Configuration for the NGramTransform Plugin.
   */
  public static class Config extends PluginConfig {

    @Description("Field to be used to transform input features into n-grams.")
    private final String fieldToBeTransformed;

    @Description("N-Gram size.")
    @Macro
    private final Integer ngramSize;

    @Description("Transformed field for sequence of n-gram.")
    private final String outputField;

    public Config(String fieldToBeTransformed, Integer ngramSize, String outputField) {
      this.fieldToBeTransformed = fieldToBeTransformed;
      this.ngramSize = ngramSize;
      this.outputField = outputField;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.fieldToBeTransformed) != null) {
      Schema.Type fieldToBeTransformedType = inputSchema.getField(config.fieldToBeTransformed)
        .getSchema().getType();
      Preconditions.checkArgument(fieldToBeTransformedType == Schema.Type.ARRAY,
                                  "Features to be transformed %s should be of type ARRAY, but was of type %s.",
                                  config.fieldToBeTransformed, fieldToBeTransformedType);
    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    //Create outputschema
    outputSchema = Schema.recordOf("outputSchema", Schema.Field.of(config.outputField,
                                                                   Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    //Schema to be used to create dataframe
    StructType schema = new StructType(new StructField[]{
      new StructField(config.fieldToBeTransformed, DataTypes.createArrayType(DataTypes.StringType),
                      false, Metadata.empty())});
    //Transform input i.e JavaRDD<StructuredRecord> to JavaRDD<Row>
    JavaRDD<Row> rowRDD = input.map(new Function<StructuredRecord, Row>() {
      @Override
      public Row call(StructuredRecord rec) throws Exception {
        return RowFactory.create(rec.get(config.fieldToBeTransformed));
      }
    });
    DataFrame wordDataFrame = sqlContext.createDataFrame(rowRDD, schema);
    NGram ngramTransformer = new NGram().setN(config.ngramSize)
      .setInputCol(config.fieldToBeTransformed).setOutputCol(config.outputField);
    DataFrame ngramDataFrame = ngramTransformer.transform(wordDataFrame);
    JavaRDD<Row> nGramRDD = javaSparkContext.parallelize(ngramDataFrame.select(config.outputField)
                                                           .collectAsList());
    //Transform JavaRDD<Row> to JavaRDD<StructuredRecord>
    JavaRDD<StructuredRecord> output = nGramRDD.map(new Function<Row, StructuredRecord>() {
      @Override
      public StructuredRecord call(Row row) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputSchema.getFields()) {
          if (row.getList(0).size() > 0) {
            builder.set(field.getName(), row.getList(0));
          }
        }
        return builder.build();
      }
    });
    return output;
  }
}
