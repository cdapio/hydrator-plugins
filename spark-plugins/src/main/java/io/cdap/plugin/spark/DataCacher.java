/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SparkCompute that caches a RDD
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("DataCacher")
@Description("Spark Data Cacher caches any incoming records and outputs them unchanged.")
public class DataCacher extends SparkCompute<StructuredRecord, StructuredRecord> {

  private final DataCacherConfig config;

  public DataCacher(DataCacherConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());

    config.validate(stageConfigurer.getFailureCollector());
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {

    StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK();
    if (config.storageLevel.isEmpty()) {
      StorageLevel.fromString(config.storageLevel);
    }
    if (storageLevel == StorageLevel.NONE()) {
      throw new RuntimeException(
        String.format("Invalid storage level '%s'. Please select a valid value", config.storageLevel));
    }

    javaRDD.persist(storageLevel);
    return javaRDD;
  }

  /**
   * Config class for DataCacher.
   */
  public static class DataCacherConfig extends PluginConfig {

    private static final String STORAGE_LEVEL = "storageLevel";


    @Name(STORAGE_LEVEL)
    @Description("Spark storage level used to cache the data")
    @Macro
    private String storageLevel;

    public void validate(FailureCollector collector) {
      if (containsMacro(STORAGE_LEVEL)) {
        return;
      }

      Set<String> allowed = new HashSet<>(Arrays.asList(
        "DISK_ONLY", "DISK_ONLY_2", "MEMORY_ONLY", "MEMORY_ONLY_2", "MEMORY_ONLY_SER",
        "MEMORY_ONLY_SER_2", "MEMORY_AND_DISK", "MEMORY_AND_DISK_2", "MEMORY_AND_DISK_SER",
        "MEMORY_AND_DISK_SER_2"));

      if (!allowed.contains(storageLevel.toUpperCase())) {
        collector
          .addFailure("Invalid value for Storage Level.",
                      "Please provide one of the following allowed values: " + allowed.toString())
          .withConfigProperty(STORAGE_LEVEL);
      }
    }
  }
}
