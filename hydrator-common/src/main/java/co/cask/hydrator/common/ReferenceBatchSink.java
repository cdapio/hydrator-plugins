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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;

import java.util.Collections;
import java.util.Map;

/**
 * A {@link BatchSink} that verifies referenceName property
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class ReferenceBatchSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {
  private final ReferencePluginConfig config;

  public ReferenceBatchSink(ReferencePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    IdUtils.validateId(config.referenceName);
    // create the external dataset this is done in the configure pipeline stage so that the external dataset with or
    // without the schema is created when the pipeline is deployed itself.
    Schema externalSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Map<String, String> dsProperties = Collections.emptyMap();
    if (externalSchema != null) {
      dsProperties = Collections.singletonMap(DatasetProperties.SCHEMA, externalSchema.toString());
    }
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE,
                                     DatasetProperties.of(dsProperties));
  }
}
