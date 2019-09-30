/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * Base streaming source that adds an External Dataset for a reference name, and performs a single getDataset()
 * call to make sure CDAP records that it was accessed.
 *
 * @param <T> type of object read by the source.
 */
public abstract class ReferenceStreamingSource<T> extends StreamingSource<T> {
  private final ReferencePluginConfig conf;

  public ReferenceStreamingSource(ReferencePluginConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    IdUtils.validateReferenceName(conf.referenceName, collector);
    // if reference name is not valid, throw an exception before creating external dataset
    collector.getOrThrowException();
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }
}
