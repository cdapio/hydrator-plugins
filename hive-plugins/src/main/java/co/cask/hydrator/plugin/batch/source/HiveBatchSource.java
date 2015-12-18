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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaConverter;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Batch source for Hive.
 */
@Plugin(type = "batchsource")
@Name("Hive")
@Description("Batch source to read from external Hive table")
public class HiveBatchSource extends BatchSource<WritableComparable, HCatRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private HiveSourceConfig config;
  private HCatRecordTransformer hCatRecordTransformer;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    //TODO CDAP-4132: remove this way of storing Hive schema once we can share info between prepareRun and initialize
    // stage.
    pipelineConfigurer.createDataset(HiveSchemaStore.HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class,
                                     DatasetProperties.EMPTY);
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
    // Use the current thread's classloader to ensure that when setInput is called it can access VersionInfo class
    // loaded above. This is needed to support CDAP 3.2 where we were just exposing classes to plugin jars and not
    // resources.
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatInputFormat.setInput(job, config.dbName, config.tableName, config.partitions);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

    HCatSchema hCatSchema = HCatInputFormat.getTableSchema(configuration);
    if (config.schema != null) {
      // if the user provided a schema then we should use that schema to read the table. This will allow user to
      // drop non-primitive types and read the table.
      hCatSchema = HiveSchemaConverter.toHiveSchema(Schema.parseJson(config.schema), hCatSchema);
      HCatInputFormat.setOutputSchema(job, hCatSchema);
    }
    HiveSchemaStore.storeHiveSchema(context, config.dbName, config.tableName, hCatSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    HCatSchema hCatSchema = HiveSchemaStore.readHiveSchema(context, config.dbName, config.tableName);
    Schema schema;
    if (config.schema == null) {
      // if the user did not provide a schema then convert the hive table's schema to cdap schema
      schema = HiveSchemaConverter.toSchema(hCatSchema);
    } else {
      schema = Schema.parseJson(config.schema);
    }
    hCatRecordTransformer = new HCatRecordTransformer(hCatSchema, schema);
  }

  @Override
  public void transform(KeyValue<WritableComparable, HCatRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = hCatRecordTransformer.toRecord(input.getValue());
    emitter.emit(record);
  }
}
