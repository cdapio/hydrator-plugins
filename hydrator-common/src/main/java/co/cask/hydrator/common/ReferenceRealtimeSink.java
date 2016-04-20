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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link RealtimeSink} that verifies referenceName property and creates an externalDataset.
 *
 * @param <I> Type of object that sink operates on
 */
public abstract class ReferenceRealtimeSink<I> extends RealtimeSink<I> {
  private final ReferencePluginConfig config;

  public ReferenceRealtimeSink(ReferencePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    @SuppressWarnings("unused")
    DatasetId datasetId = new DatasetId(NamespaceId.DEFAULT.getNamespace(), config.referenceName);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    try {
      context.provide(config.referenceName, ImmutableMap.<String, String>of());
    } catch (Throwable t) {
      // Safe to ignore since this is an hack to register lineage and we know that externalDataset will not be of
      // type KeyValueTable
    }
  }
}
