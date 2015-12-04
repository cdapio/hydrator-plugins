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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.batch.sink.TimePartitionedFileSetSink;
import co.cask.cdap.etl.common.SchemaConverter;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import co.cask.hydrator.plugin.sink.output.BulkOutputFormat;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * A sink which extends {@link TimePartitionedFileSetSink} to write to a {@link TimePartitionedFileSet} and also
 * bulk load the file to the Vertica table through copy command.
 */
@Plugin(type = "batchsink")
@Name("VerticaBulkLoad")
@Description("Batch Sink which writes to TPFS and bulk loads to a Vertica table.")
public class TPFSVerticaBulkLoadBatchSink extends TimePartitionedFileSetSink<NullWritable, Text> {

  private final VerticaBulkLoadConfig config;

  public TPFSVerticaBulkLoadBatchSink(VerticaBulkLoadConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(config.getName(), TimePartitionedFileSet.class.getName(),
                                     filesetPropertiesBuilder());
  }

  private DatasetProperties filesetPropertiesBuilder() {
    String tpfsName = config.getName();
    String basePath = config.getBasePath() == null ? tpfsName : config.getBasePath();

    FileSetProperties.Builder builder = FileSetProperties.builder();
    builder.setBasePath(basePath);
    builder.setInputFormat(TextInputFormat.class);
    builder.setOutputFormat(BulkOutputFormat.class);

    // enable explore if a schema is provided
    if (!Strings.isNullOrEmpty(config.schema)) {
      String hiveSchema;
      try {
        hiveSchema = SchemaConverter.toHiveSchema(Schema.parseJson(config.schema.toLowerCase()));
      } catch (UnsupportedTypeException | IOException e) {
        throw new RuntimeException("Error: Schema is not valid ", e);
      }
      builder.setEnableExploreOnCreate(true)
        .setExploreFormat("text")
        .setExploreFormatProperty("delimiter", config.delimiter)
        .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1));
    }
    return builder.build();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    super.prepareRun(context);
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    // set properties in the configuration which will be used by the BulkOutputCommitter while performing bulk
    // load to Vertica
    conf.set(BulkOutputFormat.VERTICA_USER_KEY, config.user);
    conf.set(BulkOutputFormat.VERTICA_PASSOWORD_KEY, config.password == null ? "" : config.password);
    conf.set(BulkOutputFormat.VERTICA_HOST_KEY, config.dbConnectionURL);
    conf.set(BulkOutputFormat.VERTICA_TABLE_NAME, config.tableName);
    conf.set(BulkOutputFormat.VERTICA_TEXT_DELIMITER, config.delimiter);
    conf.set(BulkOutputFormat.HDFS_NAMENODE_ADDR, config.hdfsNamenode == null ? "" : config.hdfsNamenode);
    conf.set(BulkOutputFormat.HDFS_NAMENODE_WEBHDFS_PORT, config.webhdfsPort == null ? "" : config.webhdfsPort);
    conf.set(BulkOutputFormat.HDFS_USER, config.hdfsUser == null ? "" : config.hdfsUser);
    conf.set(BulkOutputFormat.VERTICA_DIRECT_MODE, config.directMode == null ? Boolean.toString(true) :
      Boolean.toString(false));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String outputString = StructuredRecordStringConverter.toDelimitedString(input, config.delimiter);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(outputString)));
  }
}
