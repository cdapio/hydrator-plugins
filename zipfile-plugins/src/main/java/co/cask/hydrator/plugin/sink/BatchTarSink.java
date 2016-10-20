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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.python.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TarSink plugin which tars all the files emitted by mapper and stores tars into hdfs
 */
@Plugin(type = "batchsink")
@Name("TarSink")
@Description("TarSink plugin which tars all the files emitted by mapper and stores tars into hdfs")
public class BatchTarSink extends ReferenceBatchSink<StructuredRecord, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTarSink.class);
  static final String TAR_TARGET_PATH = "tar.target.path";
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
      conf.put(FileOutputFormat.OUTDIR, config.targetPath + "/status");
      conf.put(TAR_TARGET_PATH, config.targetPath);
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
    // TODO change this to use field names passed from config
    String fileName = input.get("fileName");
    String data = input.get("body");
    // Write to container's local directory
    LOG.info("Emitting file {}", fileName);
    emitter.emit(new KeyValue<>(new Text(fileName), new Text(data)));
  }

  /**
   * Config for TarSinkConfig.
   */
  public static class TarSinkConfig extends ReferencePluginConfig {
    @Name("targetPath")
    @Description("HDFS Destination Path Prefix.")
    @Macro
    private String targetPath;

    public TarSinkConfig(String referenceName, String targetPath) {
      super(referenceName);
      this.targetPath = targetPath;
    }
  }
}
