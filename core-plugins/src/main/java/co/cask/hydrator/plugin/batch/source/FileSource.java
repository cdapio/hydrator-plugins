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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.plugin.common.RegexFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.annotation.Nullable;

/**
 * Abstract class for defining different File sources that can exist within Hadoop. 
 * This class provides all the functionality, the extending class implements the following
 * abstract methods
 * <ul>
 *   <li>#setInputFormatProperties(Configuration configuration)</li>
 *   <li>#setInputFormatProvider(Configuration configuration)</li>*
 * </ul>
 *  
 */
@Plugin(type = "batchsource")
@Name("Text")
@Description("File source ")
public class FileSource extends BatchSource<LongWritable, Object, StructuredRecord> {
  public static final String FILE_PATTERN = "file.source.pattern";
  private FileSourceConfig config;

  /**
   * Constructor with configurations.
   * @param config plugin config.
   */
  public FileSource(FileSourceConfig config) {
    this.config = config;
  }
  
  // Specifies the output schema of this file source.
  public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  
  protected void setFileInputFormatProperties(Configuration configuration) {
    /** nothing to be done here in default mode. */
  }
  
  protected SourceInputFormatProvider setInputFormatProvider(Configuration configuration) {
    return new SourceInputFormatProvider(CombineTextInputFormat.class.getName(), configuration);
  }

  /**
   * Specifies the default regex filter for selecting the input files for processing.
   * @param configuration Hadoop configuration.
   * @return Implementation of {@link PathFilter} class.
   */
  protected Class<? extends PathFilter> setPathFilter(Configuration configuration) {
    return RegexFilter.class;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // Clear the configuration and set your configuration.
    Job job = Job.getInstance();
    Configuration configuration = job.getConfiguration();
    configuration.clear();

    // Set correct filter if pattern is set else look for all files.
    // This is the first level of filter that is being applied.
    if (config.pattern == null || config.pattern.isEmpty()) {
      configuration.set(FILE_PATTERN, ".*");
    } else {
      configuration.set(FILE_PATTERN, config.pattern);
    }

    // Sets configuration for this plugin in Hadoop configuration.
    setFileInputFormatProperties(configuration);
    
    // Sets the input path(s).
    FileInputFormat.addInputPaths(job, config.paths);
    
    // Sets the filter based on extended class implementation.
    FileInputFormat.setInputPathFilter(job, setPathFilter(configuration));
    
    // Set context input provider class.
    context.setInput(setInputFormatProvider(configuration));
    
    // Sets the split size if specified
    if (config.maxSplitSize != null) {
      FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(OUTPUT_SCHEMA)
      .set("offset", input.getKey().get())
      .set("body", input.getValue().toString())
      .build();
    emitter.emit(output);
  }

  /**
   * Basic configuration for File source.
   */
  public class FileSourceConfig extends PluginConfig {
    @Description("Specifies the input path(s). Multiple paths are separated by commas.")
    public String paths;

    @Nullable
    @Description("Specifies max split size in bytes for map size jobs. Default is 128MB.")
    public Long maxSplitSize;
    
    @Nullable
    @Description("Specifies the pattern of files to considered for processing. Default is '.*'")
    public String pattern;

    public FileSourceConfig(String paths, @Nullable Long maxSplitSize, @Nullable String pattern) {
      this.paths = paths;
      this.maxSplitSize = maxSplitSize;
      this.pattern = pattern;
    }
  }
}
