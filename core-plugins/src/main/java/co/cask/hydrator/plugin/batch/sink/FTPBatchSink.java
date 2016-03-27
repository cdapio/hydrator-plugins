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

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * FTP Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("FTP")
public class FTPBatchSink extends BatchSink<StructuredRecord, String, String> {

  private final FTPBatchSinkConfig config;

  protected FTPBatchSink(FTPBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(config.basePath, new FTPOutputFormatProvider(config, context));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<String, String>> emitter) throws Exception {
    emitter.emit(new KeyValue<>((String) input.get("body"), (String) input.get("body")));
  }

  /**
   * FTP Output Format provider.
   */
  public static class FTPOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public FTPOutputFormatProvider(FTPBatchSinkConfig config, BatchSinkContext context) {
      conf = new HashMap<>();
      conf.put(FileOutputFormat.OUTDIR, config.basePath);
    }

    @Override
    public String getOutputFormatClassName() {
      return TextOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config for FTP Batch Sink.
   */
  public static class FTPBatchSinkConfig extends PluginConfig {

    @Name("basePath")
    public String basePath;

    public FTPBatchSinkConfig() {
    }

    public FTPBatchSinkConfig(String basePath) {
      this.basePath = basePath;
    }
  }
}
