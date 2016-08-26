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

package co.cask.hydrator.common;

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;

/**
 * A {@link BatchSource} that verifies referenceName property
 *
 * @param <KEY_IN> the type of input key from the Batch run
 * @param <VAL_IN> the type of input value from the Batch run
 * @param <OUT> the type of output for the source
 */
public abstract class ReferenceBatchSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  private final ReferencePluginConfig config;

  public ReferenceBatchSource(ReferencePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    IdUtils.validateId(config.referenceName);
  }
}
