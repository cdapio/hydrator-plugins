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
import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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
  private Splitter splitter;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    if (input.isEmpty()) {
      return;
    }
    final String fieldName = config.inputCol;
    JavaRDD<Iterable<String>> textRDD = input.map(new Function<StructuredRecord, Iterable<String>>() {
      @Override
      public Iterable<String> call(StructuredRecord input) throws Exception {
        splitter = (splitter == null) ? Splitter.on(Pattern.compile(config.pattern)) : splitter;
        return SparkUtils.getInputFieldValue(input, fieldName, splitter);
      }
    });

    Word2Vec word2Vec = new Word2Vec()
      .setVectorSize(config.vectorSize)
      .setMinCount(config.minCount)
      .setNumPartitions(config.numPartitions)
      .setNumIterations(config.numIterations)
      .setWindowSize(config.windowSize);

    Word2VecModel model = word2Vec.fit(textRDD);
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(context.getSparkContext().sc(), outputFS.getBaseLocation().append(config.path).toURI().getPath());
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
    @Description("Pattern to split the input string fields on. Default is '\\s+'.")
    private String pattern;

    @Nullable
    @Description("The dimension of codes after transforming from words. Default 100.")
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
      pattern = "\\s+";
      vectorSize = 100;
      minCount = 0;
      numPartitions = 1;
      numIterations = 1;
      windowSize = 5;
    }

    public SkipGramTrainerConfig(String fileSetName, String path, String inputCol, @Nullable String pattern,
                                 @Nullable int vectorSize, @Nullable int minCount, @Nullable int numPartitions,
                                 @Nullable int numIterations, @Nullable int windowSize) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.inputCol = inputCol;
      this.pattern = pattern;
      this.vectorSize = vectorSize;
      this.minCount = minCount;
      this.numPartitions = numPartitions;
      this.numIterations = numIterations;
      this.windowSize = windowSize;
    }

    public void validate(Schema inputSchema) {
      SparkUtils.validateTextField(inputSchema, inputCol);
      try {
        Pattern.compile(pattern);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException(String.format("Invalid expression - %s. Please provide a valid pattern " +
                                                           "for splitting the string. %s.", pattern, e.getMessage()),
                                           e);
      }
      validateConfigParameters("vector size", vectorSize);
      validateConfigParameters("minimum count", minCount);
      validateConfigParameters("number of partitions", numPartitions);
      validateConfigParameters("number of iterations", numIterations);
      validateConfigParameters("window size", windowSize);
    }

    private void validateConfigParameters(String param, int value) {
      if (value < 0) {
        throw new IllegalArgumentException(String.format("Value for %s cannot be negative. Please provide a valid " +
                                                           "positive value for %s", param, param));
      }
    }
  }
}
