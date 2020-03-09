/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Source that monitors a directory and reads files from it.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("File")
@Description("File streaming source. Streams data from files that are atomically moved into a specified directory.")
public class FileStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private final Conf conf;

  public FileStreamingSource(Conf conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    conf.validate(collector);
    Schema schema = conf.getSchema(collector);
    configurer.setOutputSchema(schema);
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = conf.getSchema(context.getFailureCollector());
    // record dataset lineage
    context.registerLineage(conf.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, conf.referenceName);
      recorder.recordRead("Read", String.format("Read from files in path %s", conf.getPath()),
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    conf.getSchema(collector);
    collector.getOrThrowException();

    JavaStreamingContext jsc = context.getSparkStreamingContext();
    return FileStreamingSourceUtil.getJavaDStream(jsc, conf);
  }

  /**
   * Configuration for the source.
   */
  public static class Conf extends ReferencePluginConfig {
    private static final Set<String> FORMATS = ImmutableSet.of("text", "csv", "tsv", "clf", "grok", "syslog");
    private static final String NAME_EXTENSIONS = "extensions";
    private static final String NAME_SCHEMA = "schema";

    @Description("The format of the source files. Must be text, csv, tsv, clf, grok, or syslog. Defaults to text.")
    private String format;

    @Description("The schema of the source files.")
    private String schema;

    @Macro
    @Description("The path to the directory containing source files to stream.")
    private String path;

    @Macro
    @Description("Ignore files after they are older than this many seconds. Defaults to 60.")
    @Nullable
    private Integer ignoreThreshold;

    @Macro
    @Description("Comma separated list of file extensions to accept. If not specified, all files in the directory " +
      "will be read. Otherwise, only files with an extension in this list will be read.")
    @Nullable
    private String extensions;

    public Conf() {
      super(null);
      this.path = "";
      this.format = "text";
      this.schema = null;
      this.ignoreThreshold = 60;
      this.extensions = null;
    }

    public String getFormat() {
      return format;
    }

    public String getSchema() {
      return schema;
    }

    public String getPath() {
      return path;
    }

    @Nullable
    Integer getIgnoreThreshold() {
      return ignoreThreshold;
    }

    private void validate(FailureCollector collector) {
      if (!FORMATS.contains(format)) {
        collector.addFailure(String.format("Invalid format '%s'.", format),
                             String.format("Supported formats are: %s", Joiner.on(',').join(FORMATS)))
          .withConfigProperty(NAME_EXTENSIONS);
      }
    }

    Schema getSchema(FailureCollector collector) {
      if (Strings.isNullOrEmpty(schema)) {
        collector.addFailure("Schema must be specified.", null).withConfigProperty(NAME_SCHEMA);
        throw collector.getOrThrowException();
      }

      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
      }
      // if there was an error that was added, it will throw an exception, otherwise, this statement
      // will not be executed
      throw collector.getOrThrowException();
    }

    Set<String> getExtensions() {
      Set<String> extensionsSet = new HashSet<>();
      if (extensions == null) {
        return extensionsSet;
      }
      for (String extension : Splitter.on(',').trimResults().split(extensions)) {
        extensionsSet.add(extension);
      }
      return extensionsSet;
    }
  }
}
