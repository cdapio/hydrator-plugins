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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.IdUtils;
import co.cask.hydrator.common.ReferencePluginConfig;

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
    IdUtils.validateId(conf.referenceName);
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }
}
