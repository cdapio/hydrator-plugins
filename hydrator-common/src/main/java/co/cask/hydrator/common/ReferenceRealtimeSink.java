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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * A {@link RealtimeSink} that verifies referenceName property and creates an externalDataset.
 *
 * @param <I> Type of object that sink operates on
 */
public abstract class ReferenceRealtimeSink<I> extends RealtimeSink<I> {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRealtimeSink.class);
  private final ReferencePluginConfig config;

  public ReferenceRealtimeSink(ReferencePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Verify that reference name meets dataset id constraints
    IdUtils.validateId(config.referenceName);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    // CDAP-5759 - Remove reflection once we have a clean way to register external dataset usage
    Field workerContextField;
    try {
      workerContextField = context.getClass().getDeclaredField("context");
    } catch (NoSuchFieldException e) {
      // Should not happen, except in test cases which might invoke initialize methods and pass in mock contexts
      LOG.warn("Cannot register externalDataset usage for lineage purposes", e);
      return;
    }
    workerContextField.setAccessible(true);
    WorkerContext workerContext = (WorkerContext) workerContextField.get(context);
    workerContext.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        datasetContext.getDataset(config.referenceName);
      }
    });
  }
}
