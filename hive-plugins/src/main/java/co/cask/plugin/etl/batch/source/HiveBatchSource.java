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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.plugin.etl.batch.HiveConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Batch source for Hive.
 */
@Plugin(type = "batchsource")
@Name("Hive")
@Description("Reads from an Hive table in batch")
public class HiveBatchSource extends BatchSource<WritableComparable, HCatRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private static final Gson GSON = new Gson();
  public static final String HIVE_TABLE_SCHEMA_STORE = "hiveTableSchemaStore";
  public static final String DEFAULT_HIVE_DATABASE = "default";

  private HiveConfig config;
  private KeyValueTable hiveTableSchemaStore;
  private HCatRecordTransformer hCatRecordTransformer;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // TODO: This table is used to store the hive table schema obtained in prepareRun stage and access it back it in
    // initialize stage. Remove this once we are able to access information of prepareRun in initialize.
    pipelineConfigurer.createDataset(HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // This line is to load VersionInfo class here to make it available in the HCatInputFormat.setInput call. This is
    // needed to support CDAP 3.2 where we were just exposing the classes of the plugin jar and not the resources.
    LOG.trace("Hadoop version: {}", VersionInfo.getVersion());
    Job job = context.getHadoopJob();
    Configuration configuration = job.getConfiguration();
    job.setInputFormatClass(HCatInputFormat.class);
    configuration.set("hive.metastore.uris", config.metaStoreURI);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatInputFormat.setInput(job, Objects.firstNonNull(config.dbName, DEFAULT_HIVE_DATABASE), config.tableName,
                               config.filter);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    HCatSchema hiveSchema = HCatInputFormat.getTableSchema(configuration);
    List<HCatFieldSchema> fields = hiveSchema.getFields();
    hiveTableSchemaStore = context.getDataset(HIVE_TABLE_SCHEMA_STORE);
    hiveTableSchemaStore.write(Joiner.on(":").join(Objects.firstNonNull(config.dbName, DEFAULT_HIVE_DATABASE),
                                                   config.tableName), GSON.toJson(fields));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    hiveTableSchemaStore = context.getDataset(HIVE_TABLE_SCHEMA_STORE);
    String hiveSchema = Bytes.toString(hiveTableSchemaStore.read(Joiner.on(":").join(
      Objects.firstNonNull(config.dbName, DEFAULT_HIVE_DATABASE), config.tableName)));
    List<HCatFieldSchema> fields = GSON.fromJson(hiveSchema, new TypeToken<List<HCatFieldSchema>>() { }.getType());
    hCatRecordTransformer = new HCatRecordTransformer(new HCatSchema(fields));
  }


  @Override
  public void transform(KeyValue<WritableComparable, HCatRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = hCatRecordTransformer.toRecord(input.getValue());
    emitter.emit(record);
  }
}
