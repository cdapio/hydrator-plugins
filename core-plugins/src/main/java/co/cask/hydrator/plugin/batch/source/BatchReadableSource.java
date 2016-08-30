/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.plugin.common.BatchReadableWritableConfig;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * An abstract source for CDAP BatchReadable Datasets. Extending classes must provide implementation for
 * {@link BatchReadableSource#getProperties()} which should return the properties used by the source
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the
 */
public abstract class BatchReadableSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  private final BatchReadableWritableConfig batchReadableWritableConfig;

  protected BatchReadableSource(BatchReadableWritableConfig batchReadableWritableConfig) {
    this.batchReadableWritableConfig = batchReadableWritableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (!batchReadableWritableConfig.containsMacro(Properties.BatchReadableWritable.NAME)) {
      String datasetName = batchReadableWritableConfig.getName();
      Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");
      String datasetType = getProperties().get(Properties.BatchReadableWritable.TYPE);
      Preconditions.checkArgument(datasetType != null && !datasetType.isEmpty(), "Dataset type must be given.");

      Map<String, String> properties = Maps.newHashMap(getProperties());
      properties.remove(Properties.BatchReadableWritable.NAME);
      properties.remove(Properties.BatchReadableWritable.TYPE);

      pipelineConfigurer.createDataset(datasetName, datasetType,
                                       DatasetProperties.builder().addAll(properties).build());
    }
  }

  /**
   * An abstract method which the subclass should override to provide their dataset types
   */
  protected abstract Map<String, String> getProperties();

  @Override
  public void prepareRun(BatchSourceContext context) throws DatasetManagementException {
    Map<String, String> properties = getProperties();
    // if macros were provided at runtime, dataset needs to be created now
    if (!context.datasetExists(properties.get(Properties.BatchReadableWritable.NAME))) {
      context.createDataset(properties.get(Properties.BatchReadableWritable.NAME),
                            properties.get(Properties.BatchReadableWritable.TYPE),
                            DatasetProperties.builder().addAll(properties).build());
    }

    context.setInput(Input.ofDataset(properties.get(Properties.BatchReadableWritable.NAME)));
  }
}
