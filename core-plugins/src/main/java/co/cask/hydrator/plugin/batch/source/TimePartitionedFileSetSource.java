/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.TimeParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class to read from a {@link TimePartitionedFileSet}.
 *
 * @param <KEY> type of input key
 * @param <VALUE> type of input value
 */
public abstract class TimePartitionedFileSetSource<KEY, VALUE> extends BatchSource<KEY, VALUE, StructuredRecord> {

  private final TPFSConfig config;

  /**
   * Config for TimePartitionedFileSetDatasetAvroSource
   */
  @SuppressWarnings("unused")
  public static class TPFSConfig extends PluginConfig {
    @Description("Name of the TimePartitionedFileSet to read.")
    @Macro
    protected String name;

    @Description("Base path for the TimePartitionedFileSet. Defaults to the name of the dataset.")
    @Nullable
    @Macro
    private String basePath;

    @Description("Size of the time window to read with each run of the pipeline. " +
      "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
      "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of '5m' means each run of " +
      "the pipeline will read 5 minutes of events from the TPFS source.")
    @Macro
    private String duration;

    @Description("Optional delay for reading from TPFS source. The value must be " +
      "of the same format as the duration value. For example, a duration of '5m' and a delay of '10m' means each run " +
      "of the pipeline will read 5 minutes of data from 15 minutes before its logical start time to 10 minutes " +
      "before its logical start time. The default value is 0.")
    @Nullable
    @Macro
    private String delay;

    protected void validate() {
      // check duration and delay
      if (!containsMacro("duration")) {
        long durationInMs = TimeParser.parseDuration(duration);
        Preconditions.checkArgument(durationInMs > 0, "Duration must be greater than 0");
      }
      if (!containsMacro("delay") && !Strings.isNullOrEmpty(delay)) {
        TimeParser.parseDuration(delay);
      }
    }
  }

  public TimePartitionedFileSetSource(TPFSConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (!config.containsMacro("name") && !config.containsMacro("basePath")) {
      String tpfsName = config.name;
      FileSetProperties.Builder properties = FileSetProperties.builder();
      if (!Strings.isNullOrEmpty(config.basePath)) {
        properties.setBasePath(config.basePath);
      }
      addFileSetProperties(properties);
      pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), properties.build());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws DatasetManagementException {
    config.validate();
    // If macros provided at runtime, dataset still needs to be created
    if (!context.datasetExists(config.name)) {
      String tpfsName = config.name;
      FileSetProperties.Builder properties = FileSetProperties.builder();
      if (!Strings.isNullOrEmpty(config.basePath)) {
        properties.setBasePath(config.basePath);
      }
      addFileSetProperties(properties);
      context.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), properties.build());
    }

    long duration = TimeParser.parseDuration(config.duration);
    long delay = Strings.isNullOrEmpty(config.delay) ? 0 : TimeParser.parseDuration(config.delay);
    long endTime = context.getLogicalStartTime() - delay;
    long startTime = endTime - duration;
    Map<String, String> sourceArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(sourceArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(sourceArgs, endTime);
    context.setInput(Input.ofDataset(config.name, sourceArgs));
  }

  /**
   * Set file set specific properties, such as input/output format and explore properties.
   */
  protected abstract void addFileSetProperties(FileSetProperties.Builder properties);
}
