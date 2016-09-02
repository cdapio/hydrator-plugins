/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaConverter;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaStore;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hive.service.auth.HiveAuthFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive Batch Sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Hive")
@Description("Batch Sink to write to external Hive tables.")
public class HiveBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, HCatRecord> {

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private HiveSinkConfig config;
  private RecordToHCatRecordTransformer recordToHCatRecordTransformer;

  public HiveBatchSink(HiveSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    //TODO CDAP-4132: remove this way of storing Hive schema once we can share info between prepareRun and initialize
    // stage.
    pipelineConfigurer.createDataset(HiveSchemaStore.HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class,
                                     DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = JobUtils.createInstance();

    HiveSinkOutputFormatProvider sinkOutputFormatProvider = new HiveSinkOutputFormatProvider(job, config);
    HCatSchema hiveSchema = sinkOutputFormatProvider.getHiveSchema();
    HiveSchemaStore.storeHiveSchema(context, config.dbName, config.tableName, hiveSchema);
    context.addOutput(Output.of(config.referenceName, sinkOutputFormatProvider));
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

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, HCatRecord>> emitter) throws Exception {
    HCatRecord hCatRecord = recordToHCatRecordTransformer.toHCatRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), hCatRecord));
  }

  /**
   * Output format provider for Hive Sink
   */
  public static class HiveSinkOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;
    private HCatSchema hiveSchema;

    public HiveSinkOutputFormatProvider(Job job, HiveSinkConfig config) throws IOException {
      Configuration originalConf = job.getConfiguration();
      Configuration modifiedConf = new Configuration(originalConf);
      modifiedConf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.metaStoreURI);
      if (UserGroupInformation.isSecurityEnabled()) {
        addSecureHiveProperties(modifiedConf);
      }
      HCatOutputFormat.setOutput(modifiedConf, job.getCredentials(), OutputJobInfo.create(config.dbName,
                                                                                          config.tableName,
                                                                                          getPartitions(config)));

      hiveSchema = HCatOutputFormat.getTableSchema(modifiedConf);

      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(modifiedConf);
      // if dynamic partitioning was used then append the dynamic partitioning columns to the table schema obtained from
      // hive as the schema obtained does not have these columns. The partition columns which are static
      // does not need to be appended since they are not expected to be present in the incoming record.
      if (jobInfo.isDynamicPartitioningUsed()) {
        HCatSchema partitionColumns = jobInfo.getTableInfo().getPartitionColumns();
        List<String> dynamicPartitioningKeys = jobInfo.getDynamicPartitioningKeys();

        for (String dynamicPartitioningKey : dynamicPartitioningKeys) {
          HCatFieldSchema curFieldSchema = partitionColumns.get(dynamicPartitioningKey);
          hiveSchema.append(curFieldSchema);
        }
      }

      if (config.schema != null) {
        // if the user did provide a sink schema to use then use that one
        hiveSchema = HiveSchemaConverter.toHiveSchema(Schema.parseJson(config.schema), hiveSchema);
      }
      HCatOutputFormat.setSchema(modifiedConf, hiveSchema);
      conf = getConfigurationDiff(originalConf, modifiedConf);
    }

    private void addSecureHiveProperties(Configuration modifiedConf) throws IOException {
      modifiedConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");

      // HCatalogOutputFormat reads Hive delegation token with empty service name
      // because Oozie passes the Hive delegation with empty service name.
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      TokenSelector<? extends TokenIdentifier> hiveTokenSelector = new DelegationTokenSelector();
      Token<? extends TokenIdentifier> hiveToken = hiveTokenSelector.selectToken(
        new Text(HiveAuthFactory.HS2_CLIENT_TOKEN), ugi.getTokens());
      Token<? extends TokenIdentifier> noServiceHiveToken = new Token<>(hiveToken);
      noServiceHiveToken.setService(new Text());
      ugi.addToken(noServiceHiveToken);
    }

    private Map<String, String> getConfigurationDiff(Configuration originalConf, Configuration modifiedConf) {
      MapDifference<String, String> mapDifference = Maps.difference(getConfigurationAsMap(originalConf),
                                                                    getConfigurationAsMap(modifiedConf));
      // new keys in the modified configurations
      Map<String, String> newEntries = mapDifference.entriesOnlyOnRight();
      // keys whose values got changed in the modified config
      Map<String, MapDifference.ValueDifference<String>> stringValueDifferenceMap = mapDifference.entriesDiffering();
      Map<String, String> result = new HashMap<>();
      for (Map.Entry<String, MapDifference.ValueDifference<String>> stringValueDifferenceEntry :
        stringValueDifferenceMap.entrySet()) {
        result.put(stringValueDifferenceEntry.getKey(), stringValueDifferenceEntry.getValue().rightValue());
      }
      result.putAll(newEntries);
      return result;
    }

    private Map<String, String> getPartitions(HiveSinkConfig config) {
      Map<String, String> partitionValues = null;
      if (config.partitions != null) {
        partitionValues = GSON.fromJson(config.partitions, STRING_MAP_TYPE);
      }
      return partitionValues;
    }

    private Map<String, String> getConfigurationAsMap(Configuration conf) {
      Map<String, String> map = new HashMap<>();
      for (Map.Entry<String, String> entry : conf) {
        map.put(entry.getKey(), entry.getValue());
      }
      return map;
    }

    /**
     * @return the {@link HCatSchema} for the Hive table for this {@link HiveSinkOutputFormatProvider}
     */
    public HCatSchema getHiveSchema() {
      return hiveSchema;
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
