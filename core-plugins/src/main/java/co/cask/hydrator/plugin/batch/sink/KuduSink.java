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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("KuduSink")
@Description("Sink that outputs to a Kudu instance")
public class KuduSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Text> {

private final KuduSinkConfig config;

  public KuduSink(ReferencePluginConfig config, KuduSinkConfig config1) {
    super(config);
    this.config = config1;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

    @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    batchSinkContext.addOutput(Output.of(config.referenceName, new KuduOutputFormatProvider(config)));
  }

  private static class KuduOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    private KuduOutputFormatProvider(KuduSinkConfig config) {
      this.conf = new HashMap<>();
    }


    @Override
    public String getOutputFormatClassName() {
      return KuduOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   *
   */
  public static class KuduSinkConfig extends ReferencePluginConfig {

    public KuduSinkConfig(String referenceName) {
      super(referenceName);
    }
  }
}
