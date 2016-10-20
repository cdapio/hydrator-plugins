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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.io.Text;
import org.python.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * TarSink plugin which tars all the files emitted by mapper and stores tars into hdfs
 */
@Plugin(type = "batchsink")
@Name("TarSink")
@Description("TarSink plugin which tars all the files emitted by mapper and stores tars into hdfs")
public class BatchTarSink extends ReferenceBatchSink<StructuredRecord, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTarSink.class);
  static final String TAR_TARGET_PATH = "tar.target.path";
  static final String SUPPORTED_EXTN = "supported.extn";
  private TarSinkConfig config;

  @VisibleForTesting
  public BatchTarSink(TarSinkConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Tar Sink Output Provider.
   */
  public static class SinkOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;
    private final TarSinkConfig config;

    public SinkOutputFormatProvider(TarSinkConfig config, BatchSinkContext context) {
      this.conf = new HashMap<>();
      this.config = config;
      LOG.info("TargetPath from config: {}", config.targetPath);
      conf.put(TAR_TARGET_PATH, config.targetPath);
      conf.put(SUPPORTED_EXTN, config.listOfFileExtensions);
    }

    @Override
    public String getOutputFormatClassName() {
      return outputFormatClassName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  private static String outputFormatClassName() {
    // Use custom output format
    return BatchTarSinkOutputFormat.class.getName();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(config, context)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, Text>> emitter) throws Exception {
    String fileName = input.get(config.fileNameField);
    String data = input.get(config.bodyField);
    Set<String> fileExtensions = config.getFileExtensions();
    // Write to container's local directory
    LOG.info("Emitting file {}", fileName);
    String extension = fileName.substring(fileName.lastIndexOf(".") + 1);
    LOG.info("Extension is {}", extension);
    if (fileExtensions.contains(extension)) {
      emitter.emit(new KeyValue<>(new Text(fileName), new Text(data)));
    }
  }

  /**
   * Config for TarSinkConfig.
   */
  public static class TarSinkConfig extends ReferencePluginConfig {
    @Name("targetPath")
    @Description("HDFS Destination Path Prefix.")
    @Macro
    private String targetPath;

    @Name("fileNameField")
    @Description("field name for file")
    private String fileNameField;

    @Name("bodyField")
    @Description("List of file extensions separated by comma which needs to be used in tar")
    @Macro
    private String bodyField;

    @Name("listOfFileExtensions")
    @Description("field name for data")
    private String listOfFileExtensions;

    public TarSinkConfig(String referenceName, String targetPath, String fileNameField, String bodyField,
                         String listOfFileExtensions) {
      super(referenceName);
      this.targetPath = targetPath;
      this.fileNameField = fileNameField;
      this.bodyField = bodyField;
      this.listOfFileExtensions = listOfFileExtensions;
    }

    private Set<String> getFileExtensions() {
      Set<String> set = new HashSet<>();
      for (String field : Splitter.on(',').trimResults().split(listOfFileExtensions)) {
        set.add(field);
      }
      return ImmutableSet.copyOf(set);
    }
  }
}
