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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.hydrator.plugin.common.AvroToStructuredTransformer;
import co.cask.hydrator.plugin.common.FileSetUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

/**
 * A {@link BatchSource} to read Avro record from {@link TimePartitionedFileSet}
 */
@Plugin(type = "batchsource")
@Name("TPFSAvro")
@Description("Reads from a TimePartitionedFileSet whose data is in Avro format.")
public class TimePartitionedFileSetDatasetAvroSource extends
  TimePartitionedFileSetSource<AvroKey<GenericRecord>, NullWritable> {
  private final TPFSAvroConfig tpfsAvroConfig;

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetAvroSource
   */
  public static class TPFSAvroConfig extends TPFSConfig {

    @Description("The Avro schema of the record being read from the source as a JSON Object.")
    private String schema;

    @Override
    protected void validate() {
      super.validate();
      try {
        new Schema.Parser().parse(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tpfsAvroConfig.schema), "Schema must be specified.");
    try {
      co.cask.cdap.api.data.schema.Schema schema = co.cask.cdap.api.data.schema.Schema.parseJson(tpfsAvroConfig.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  public TimePartitionedFileSetDatasetAvroSource(TPFSAvroConfig tpfsAvroConfig) {
    super(tpfsAvroConfig);
    this.tpfsAvroConfig = tpfsAvroConfig;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    FileSetUtil.configureAvroFileSet(tpfsAvroConfig.schema, properties);
  }

  @Override
  public void transform(KeyValue<AvroKey<GenericRecord>, NullWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(recordTransformer.transform(input.getKey().datum()));
  }
}
