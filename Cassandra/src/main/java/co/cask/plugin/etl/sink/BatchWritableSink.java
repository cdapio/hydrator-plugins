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

package co.cask.plugin.etl.sink;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * This class is copied from CDAP for testing.
 * An abstract Sink for CDAP Datasets that are batch writable, which means they can be used as output of a
 * mapreduce job. Extending subclasses must provide implementation for {@link BatchWritableSink} which should return the
 * properties used by the sink.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class BatchWritableSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String datasetName = getProperties().get(BatchReadableWritable.NAME);
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");
    String datasetType = getProperties().get(BatchReadableWritable.TYPE);
    Preconditions.checkArgument(datasetType != null && !datasetType.isEmpty(), "Dataset type must be given.");

    Map<String, String> properties = Maps.newHashMap(getProperties());
    properties.remove(BatchReadableWritable.NAME);
    properties.remove(BatchReadableWritable.TYPE);

    pipelineConfigurer.createDataset(datasetName, datasetType, DatasetProperties.builder().addAll(properties).build());
  }

  /**
   * An abstract method which the subclass should override to provide their dataset types
   */
  protected abstract Map<String, String> getProperties();

  @Override
  public void prepareRun(BatchSinkContext context) {
    PluginProperties pluginProperties = context.getPluginProperties();
    context.setOutput(pluginProperties.getProperties().get(BatchReadableWritable.NAME));
  }

  /**
   * Common properties for BatchWritable source and sinks
   */
  public static class BatchReadableWritable {
    public static final String NAME = "name";
    public static final String TYPE = "type";
  }
}
