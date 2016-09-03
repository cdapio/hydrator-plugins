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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link GCSAvroBatchSink} that stores data in avro format to Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSAvro")
@Description("Batch sink to write to Google Cloud Storage in Avro format.")
public class GCSAvroBatchSink extends GCSBatchSink<AvroKey<GenericRecord>, NullWritable> {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private StructuredToAvroTransformer recordTransformer;
  private final GCSAvroSinkConfig config;

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
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context,
                                                            String fileSystemProperties) {
    return new GCSAvroOutputFormatProvider(config, fileSystemProperties);
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
    @Description("The Avro schema of the record being written to the sink as a JSON object")
    private String schema;

    @VisibleForTesting
    public GCSAvroSinkConfig(String referenceName, String bucketKey, String schema, String projectId,
                             String serviceKeyFile,
                             String filesystemProperties, String path) {
      super(referenceName, bucketKey, projectId, serviceKeyFile,
            filesystemProperties, path);
      this.schema = schema;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class GCSAvroOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public GCSAvroOutputFormatProvider(GCSAvroSinkConfig config, String fileSystemProperties) {
      conf = new HashMap<>();
      conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
      conf.put("avro.schema.output.key", config.schema);
      if (fileSystemProperties != null) {
        Map<String, String> propertyMap = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
        conf.putAll(propertyMap);
      }
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

