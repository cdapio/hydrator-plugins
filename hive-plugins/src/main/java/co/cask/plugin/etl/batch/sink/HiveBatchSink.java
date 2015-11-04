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

package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.plugin.etl.batch.commons.HiveSchemaConverter;
import co.cask.plugin.etl.batch.commons.HiveSchemaStore;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Hive Batch Sink
 */
@Plugin(type = "batchsink")
@Name("Hive")
@Description("Batch Sink to write to external Hive tables.")
public class HiveBatchSink extends BatchSink<StructuredRecord, NullWritable, HCatRecord> {

  private static final String DEFAULT_HIVE_DATABASE = "default";
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();

  private HiveSinkConfig config;
  private RecordToHCatRecordTransformer recordToHCatRecordTransformer;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    //TODO CDAP-4132: remove this way of storing Hive schema once we can share info between prepareRun and initialize
    // stage.
    pipelineConfigurer.createDataset(HiveSchemaStore.HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class,
                                     DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    Configuration configuration = job.getConfiguration();
    configuration.set("hive.metastore.uris", config.metaStoreURI);
    HCatOutputFormat.setOutput(configuration, job.getCredentials(), OutputJobInfo.create(config.dbName,
                                                                                         config.tableName,
                                                                                         getPartitions()));

    HCatSchema hiveSchema = HCatOutputFormat.getTableSchema(configuration);
    if (config.schema != null) {
      // if the user did provide a sink schema to use then use that one
      hiveSchema = HiveSchemaConverter.toHiveSchema(Schema.parseJson(config.schema), hiveSchema);
    }
    HCatOutputFormat.setSchema(configuration, hiveSchema);
    HiveSchemaStore.storeHiveSchema(context, config.dbName, config.tableName, hiveSchema);
    context.addOutput(config.tableName, new SinkOutputFormatProvider());
  }

  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    HCatSchema hCatSchema = HiveSchemaStore.readHiveSchema(context, config.dbName, config.tableName);
    Schema schema;
    // if the user did not provide a source schema then get the schema from the hive's schema
    if (config.schema == null) {
      schema = HiveSchemaConverter.toSchema(hCatSchema);
    } else {
      schema = Schema.parseJson(config.schema);
    }
    recordToHCatRecordTransformer = new RecordToHCatRecordTransformer(hCatSchema, schema);
  }

  private Map<String, String> getPartitions() {
    Map<String, String> partitionValues = null;
    if (config.partitions != null) {
      partitionValues = GSON.fromJson(config.partitions, STRING_MAP_TYPE);
    }
    return partitionValues;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, HCatRecord>> emitter) throws Exception {
    HCatRecord hCatRecord = recordToHCatRecordTransformer.toHCatRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), hCatRecord));
  }

  /**
   * Output format provider for Hive Sink
   */
  public static class SinkOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    public SinkOutputFormatProvider() {
      this.conf = new HashMap<>();
    }

    @Override
    public String getOutputFormatClassName() {
      return HCatOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
