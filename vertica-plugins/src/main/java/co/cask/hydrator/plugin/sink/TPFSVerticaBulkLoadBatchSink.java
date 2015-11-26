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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.batch.sink.TimePartitionedFileSetSink;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import co.cask.hydrator.plugin.sink.output.BulkOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A sink which extends {@link TimePartitionedFileSetSink} to write to a {@link TimePartitionedFileSet} and also
 * bulk load the file to the Vertica table through copy command.
 */
@Plugin(type = "batchsink")
@Name("Vertica")
@Description("Batch Sink which writes to TPFS and bulk loads to a vertica table.")
public class TPFSVerticaBulkLoadBatchSink extends TimePartitionedFileSetSink<NullWritable, Text> {

  private final VerticaBulkLoadConfig config;

  protected TPFSVerticaBulkLoadBatchSink(VerticaBulkLoadConfig config) {
    super(config);
    this.config = config;
  }


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    String tpfsName = config.getName();
    String basePath = config.getBasePath() == null ? tpfsName : config.getBasePath();
    // parse it to make sure its valid
//    new Schema.Parser().parse(config.schema);
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(BulkOutputFormat.class)
      .build());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String outputString = StructuredRecordStringConverter.toDelimitedString(input, "|");
    System.out.println("### The string is: " + outputString);
    emitter.emit(new KeyValue<NullWritable, Text>(NullWritable.get(), new Text(outputString)));
  }
}
