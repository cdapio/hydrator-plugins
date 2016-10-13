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
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Tokenizer-SparkCompute that breaks text(such as sentence) into individual terms(usually words)
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(Tokenizer.PLUGIN_NAME)
@Description("Used to tokenize text(such as sentence) into individual terms(usually words)")
public class Tokenizer extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "Tokenizer";
  private List<String> fieldList = new ArrayList<>();
  private Config config;
  private Schema outputSchema;
  private StructType schema;

  public Tokenizer(Config config) {
    this.config = config;
  }

  /**
   * Configuration for the Tokenizer Plugin.
   */
  public static class Config extends PluginConfig {
    @Description("Column on which tokenization is to be done")
    private final String columnToBeTokenized;

    @Description("Pattern Separator")
    private final String patternSeparator;

    @Description("Output column name for tokenized data")
    private final String outputColumn;

    public Config(String outputColumn, String columnToBeTokenized, String patternSeparator) {
      this.columnToBeTokenized = columnToBeTokenized;
      this.patternSeparator = patternSeparator;
      this.outputColumn = outputColumn;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.columnToBeTokenized) != null) {
      Schema.Type type = inputSchema.getField(config.columnToBeTokenized)
        .getSchema().getType();
      Preconditions.checkArgument(type == Schema.Type.STRING,
                                  "Column to be tokenized %s must be of type String, but was of type %s.",
                                  config.columnToBeTokenized, type);
    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (input == null) {
      return context.getSparkContext().emptyRDD();
    }
    if (fieldList.isEmpty()) {
      for (Schema.Field field : input.first().getSchema().getFields()) {
        fieldList.add(field.getName());
      }
      fieldList.add(config.outputColumn);
    }
    Schema inputSchema = input.first().getSchema();
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    List<StructField> fields = new ArrayList<StructField>();
    for (Schema ts : new Schema[]{inputSchema}) {
      for (Schema.Field field : ts.getFields()) {
        field.getSchema().getType();
        fields.add(DataTypes.createStructField(field.getName().toLowerCase(), getDataType(field.getSchema()), true));
      }
    }
    //Schema to be used to create dataframe
    schema = DataTypes.createStructType(fields);
    //Transform input i.e JavaRDD<StructuredRecord> to JavaRDD<Row>
    JavaRDD<Row> rowRDD = input.map(new Function<StructuredRecord, Row>() {
      @Override
      public Row call(StructuredRecord record) throws Exception {
        List<Object> fields = new ArrayList<>();
        for (String field : schema.fieldNames()) {
          fields.add(record.get(field));
        }
        return RowFactory.create(fields.toArray());
      }
    });
    DataFrame sentenceDataFrame = sqlContext.createDataFrame(rowRDD, schema);
    RegexTokenizer tokenizer = new RegexTokenizer().setInputCol(config.columnToBeTokenized)
      .setOutputCol(config.outputColumn)
      .setPattern(config.patternSeparator);
    DataFrame tokenizedDataFrame = tokenizer.transform(sentenceDataFrame);
    JavaRDD<Row> tokenizedRDD =
      javaSparkContext.parallelize(tokenizedDataFrame.collectAsList());
    outputSchema = outputSchema != null ? outputSchema : getOutputSchema(inputSchema);
    //Transform JavaRDD<Row> to JavaRDD<StructuredRecord>
    final JavaRDD<StructuredRecord> output = tokenizedRDD.map(new Function<Row, StructuredRecord>() {
      @Override
      public StructuredRecord call(Row row) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        String[] fields = fieldList.toArray(new String[fieldList.size()]);
        for (int i = 0; i < fields.length; i++) {
          Schema.Type type = outputSchema.getField(fields[i]).getSchema().getType();
          if (type.equals(Schema.Type.ARRAY)) {
            builder.set(fields[i], row.getList(i));
          } else if (type.equals(Schema.Type.MAP)) {
            builder.set(fields[i], row.getJavaMap(i));
          } else {
            builder.set(fields[i], row.get(i));
          }
        }
        return builder.build();
      }
    });
    return output;
  }

  private Schema getOutputSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(config.outputColumn, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf("record", fields);
  }

  private static DataType getDataType(Schema schema) {
    DataType returnDataType = DataTypes.StringType;
    switch (schema.getType()) {
      case INT:
        returnDataType = DataTypes.IntegerType;
        break;
      case STRING:
        returnDataType = DataTypes.StringType;
        break;
      case BOOLEAN:
        returnDataType = DataTypes.BooleanType;
        break;
      case BYTES:
        returnDataType = DataTypes.ByteType;
        break;
      case DOUBLE:
        returnDataType = DataTypes.DoubleType;
        break;
      case FLOAT:
        returnDataType = DataTypes.FloatType;
        break;
      case LONG:
        returnDataType = DataTypes.LongType;
        break;
      case ENUM:
        returnDataType = DataTypes.StringType;
        break;
    }
    return returnDataType;
  }
}
