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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model using SkipGram (Spark's Word2Vec).
 * Saves this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(SkipGramTrainer.PLUGIN_NAME)
@Description("Fits a model using SkipGram.")
public class SkipGramTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "SkipGramTrainer";
  private SkipGramTrainerConfig config;
  private StructType schema;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {
    if (input == null) {
      return;
    }

    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    Set<String> inputCols = new LinkedHashSet<>();
    inputCols.add(config.inputCol);
    List<StructField> structField = SparkUtils.getStructFieldList(input.first().getSchema().getFields(), inputCols);
    schema = new StructType(structField.toArray(new StructField[structField.size()]));
    JavaRDD<Row> rowRDD = SparkUtils.convertJavaRddStructuredRecordToJavaRddRows(input, schema);
    DataFrame documentDF = sqlContext.createDataFrame(rowRDD, schema);

    Word2Vec word2Vec = new Word2Vec()
      .setInputCol(config.inputCol + "-transformed")
      .setVectorSize(config.vectorSize)
      .setMinCount(config.minCount)
      .setNumPartitions(config.numPartitions)
      .setMaxIter(config.numIterations)
      .setWindowSize(config.windowSize);

    Word2VecModel model = word2Vec.fit(documentDF);
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(outputFS.getBaseLocation().append(config.path).toURI().getPath());
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    //no-op
  }

  /**
   * Configuration for SkipGramTrainer.
   */
  public static class SkipGramTrainerConfig extends PluginConfig {
    @Description("The name of the FileSet to save the model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Description("Field to be used for training.")
    private String inputCol;

    @Nullable
    @Description("The dimension of codes after transforming from words. Default 3.")
    private Integer vectorSize;

    @Nullable
    @Description("The minimum number of times a token must appear to be included in the word2vec model's vocabulary. " +
      "Default 0.")
    private Integer minCount;

    @Nullable
    @Description("Sets number of partitions. Use a small number for accuracy. Default is 1.")
    private Integer numPartitions;

    @Nullable
    @Description("Maximum number of iterations (>= 0). Default 1.")
    private Integer numIterations;

    @Description("The window size (context words from [-window, window]). Default 5.")
    @Nullable
    private Integer windowSize;

    public SkipGramTrainerConfig() {
      vectorSize = 3;
      minCount = 0;
      numPartitions = 1;
      numIterations = 1;
      windowSize = 5;
    }

    public SkipGramTrainerConfig(String fileSetName, String path, String inputCol, @Nullable int vectorSize,
                                 @Nullable int minCount, @Nullable int numPartitions, @Nullable int numIterations,
                                 @Nullable int windowSize) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.inputCol = inputCol;
      this.vectorSize = vectorSize;
      this.minCount = minCount;
      this.numPartitions = numPartitions;
      this.numIterations = numIterations;
      this.windowSize = windowSize;
    }

    private void validate(Schema inputSchema) {
      Schema.Field field = inputSchema.getField(inputCol);
      if (field == null) {
        throw new IllegalArgumentException(String.format("Input column %s does not exist in the input schema",
                                                         inputCol));
      }
      Schema schema = field.getSchema();
      Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
      if (!(type.equals(Schema.Type.STRING) || type.equals(Schema.Type.BOOLEAN) || (type.equals(Schema.Type.ARRAY) &&
        (schema.getComponentSchema().getType().equals(Schema.Type.STRING) ||
          schema.getComponentSchema().getType().equals(Schema.Type.BOOLEAN))))) {
        throw new IllegalArgumentException(String.format("Input column can be of type string, boolean or array of " +
                                                           "string or boolean. But was %s for %s.", type, inputCol));
      }

    }
  }
}
