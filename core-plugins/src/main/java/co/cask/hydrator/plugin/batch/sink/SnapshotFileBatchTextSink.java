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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.plugin.common.StructuredToTextTransformer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in text format.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SnapshotText")
@Description("Sink for a SnapshotFileSet that writes data in Text format.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchTextSink extends SnapshotFileBatchSink<String, NullWritable> {
  private StructuredToTextTransformer recordTransformer;
  private SnapshotFileBatchTextSinkConfig config;

  public SnapshotFileBatchTextSink(SnapshotFileBatchTextSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToTextTransformer(config.delimiter);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<String, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(recordTransformer.transform(input), NullWritable.get()));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    propertiesBuilder
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema("text string");
  }

  /**
   * Config for SnapshotFileBatchTextSink.
   */
  public static class SnapshotFileBatchTextSinkConfig extends SnapshotFileSetBatchSinkConfig {
    @Description("The Delimiter used to combine the Structured Record fields, Defaults to tab")
    @Nullable
    @Macro
    private String delimiter;

    public SnapshotFileBatchTextSinkConfig() {
      this.delimiter = "\t";
    }

    public SnapshotFileBatchTextSinkConfig(String name, @Nullable String basePath, @Nullable String delimiter) {
      super(name, basePath, null);
      this.delimiter = delimiter;
    }
  }
}
