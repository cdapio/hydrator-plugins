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

package co.cask.hydrator.plugin.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "batchsource")
@Name("ZipFile")
@Description("Batch source for File Systems")
public class ZipFileBatchSource extends ReferenceBatchSource<String, BytesWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ZipFileBatchSource.class);

  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("fileName", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come" +
    " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where" +
    " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'.";

  private final ZipFileBatchConfig config;

  public ZipFileBatchSource(ZipFileBatchConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    conf.set(INPUT_NAME_CONFIG, config.path);
    FileInputFormat.addInputPath(job, new Path(config.path));
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(CompleteFileInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<String, BytesWritable> input, Emitter<StructuredRecord> emitter) throws Exception {
    String fileName = input.getKey();
    LOG.info("FileName is {}", fileName);

    // zip file content
    byte[] fileContent = input.getValue().getBytes();

    String cwd = System.getProperty("user.dir");
    File outputDir = new File(cwd + "/zipfile");

    outputDir.mkdir();
    LOG.info("Created Dir");
    File outputFile = new File(outputDir.getAbsolutePath() + "/" + fileName);
    InputStream inputStream = new ByteArrayInputStream(fileContent);
    Files.copy(inputStream, outputFile.toPath());
    LOG.info("Copied File to dir");

    File extractDir = new File(outputDir.getAbsolutePath() + "/extract");
    extractDir.mkdir();
    ProcessBuilder processBuilder = new ProcessBuilder("unzip", outputFile.getAbsolutePath(),
                                                       "-d", extractDir.getAbsolutePath());
    processBuilder.directory(outputDir);
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();
    int exitValue = process.waitFor();
    LOG.info("Exit value of Process is {}", exitValue);
    LOG.info("Extraction complete {}", exitValue);
    for (File file : extractDir.listFiles()) {
      LOG.info("Extracted File : {}", file.getName());
      // emit record with file name as key and file content as body.
      StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
        .set("fileName", file.getName())
        .set("body", new String(Files.readAllBytes(file.toPath()), Charsets.UTF_8))
        .build();
      emitter.emit(output);
      LOG.info("Emitted File {} as Record", file.getName());
    }
  }

  @VisibleForTesting
  ZipFileBatchConfig getConfig() {
    return config;
  }

  /**
   * Config class that contains all the properties needed for the file source.
   */
  public static class ZipFileBatchConfig extends ReferencePluginConfig {
    @Description(PATH_DESCRIPTION)
    @Macro
    public String path;

    public ZipFileBatchConfig() {
      super("");
    }

    public ZipFileBatchConfig(String referenceName, String path) {
      super(referenceName);
      this.path = path;
    }
  }
}
