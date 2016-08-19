
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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToOrcTransformer;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write ORC records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("TPFSOrc")
@Description("Sink for a TimePartitionedFileSet that writes data in ORC format.")
public class TimePartitionedFileSetDataSetORCSink extends TimePartitionedFileSetSink<NullWritable, OrcStruct> {

  private static final String SCHEMA_DESC = "The ORC schema of the record being written to the Sink.";
  private final TPFSOrcSinkConfig config;
  private StructuredToOrcTransformer recordTransformer;

  public TimePartitionedFileSetDataSetORCSink(TPFSOrcSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToOrcTransformer();
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureORCFileSet(config.schema, properties);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, OrcStruct>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), recordTransformer.transform(input, input.getSchema())));
  }

  /**
   * Config for TimePartitionedFileSetOrcSink
   */
  public static class TPFSOrcSinkConfig extends TPFSSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;

    public TPFSOrcSinkConfig(String name, String schema, @Nullable String basePath, @Nullable String pathFormat,
                             @Nullable String timeZone) {
      super(name, basePath, pathFormat, timeZone);
      this.schema = schema;
    }
  }
}
