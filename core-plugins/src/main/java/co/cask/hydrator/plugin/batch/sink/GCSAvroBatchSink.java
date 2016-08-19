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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;

/**
 * {@link GCSAvroBatchSink} that stores data in avro format to Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSAvro")
@Description("Batch sink to write to Google Cloud Storage in Avro format.")
public class GCSAvroBatchSink extends GCSBatchSink<AvroKey<GenericRecord>, NullWritable> {

  private StructuredToAvroTransformer recordTransformer;
  private final GCSAvroSinkConfig config;

  private static final String SCHEMA_DESC = "The Avro schema of the record being written to the sink as a JSON " +
    "object.";

  public GCSAvroBatchSink(GCSAvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context) {
    return new GCSAvroOutputFormatProvider(config, context);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Configuration for the GCSAvroSink.
   */
  public static class GCSAvroSinkConfig extends GCSSinkConfig {

    @Name("schema")
    @Description(SCHEMA_DESC)
    private String schema;

    @SuppressWarnings("unused")
    public GCSAvroSinkConfig() {
      super();
    }

    @SuppressWarnings("unused")
    public GCSAvroSinkConfig(String referenceName, String bucketKey, String schema, String projectId,
                             String serviceEmail,  String serviceKeyFile, String filesystemProperties) {
      super(referenceName, bucketKey, projectId, serviceEmail, serviceKeyFile, filesystemProperties);
      this.schema = schema;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class GCSAvroOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public GCSAvroOutputFormatProvider(GCSAvroSinkConfig config, BatchSinkContext context) {
      conf = Maps.newHashMap();
      conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
      conf.put("avro.schema.output.key", config.schema);
      conf.put(FileOutputFormat.OUTDIR,
               String.format("gs://%s", config.bucketKey));

    }

    @Override
    public String getOutputFormatClassName() {
      return AvroKeyOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}

