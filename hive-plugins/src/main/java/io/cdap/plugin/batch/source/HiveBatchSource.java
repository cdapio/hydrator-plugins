/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.batch.commons.HiveSchemaConverter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Batch source for Hive.
 */
@Plugin(type = "batchsource")
@Name("Hive")
@Description("Batch source to read from external Hive table")
public class HiveBatchSource extends ReferenceBatchSource<WritableComparable, HCatRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private static final Gson GSON = new Gson();
  private HiveSourceConfig config;
  private HCatRecordTransformer hCatRecordTransformer;

  public HiveBatchSource(HiveSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (config.schema != null) {
      try {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // TODO: remove after CDAP-5950 is fixed
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      Job job = JobUtils.createInstance();
      Configuration conf = job.getConfiguration();

      conf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.metaStoreURI);

      if (UserGroupInformation.isSecurityEnabled()) {
        conf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
        conf.set("hive.metastore.token.signature", HiveAuthFactory.HS2_CLIENT_TOKEN);
      }
      HCatInputFormat.setInput(conf, config.dbName, config.tableName, config.partitions);

      HCatSchema hCatSchema = HCatInputFormat.getTableSchema(conf);
      if (config.getSchema() != null) {
        // if the user provided a schema then we should use that schema to read the table. This will allow user to
        // drop non-primitive types and read the table.
        hCatSchema = HiveSchemaConverter.toHiveSchema(config.getSchema(), hCatSchema);
        HCatInputFormat.setOutputSchema(job, hCatSchema);
      }
      context.getArguments().set(config.getDBTable(), GSON.toJson(hCatSchema));
      LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
      lineageRecorder.createExternalDataset(config.getSchema());
      context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(HCatInputFormat.class, conf)));
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    HCatSchema hCatSchema = GSON.fromJson(context.getArguments().get(config.getDBTable()), HCatSchema.class);
    Schema schema;
    if (config.schema == null) {
      // if the user did not provide a schema then convert the hive table's schema to cdap schema
      schema = HiveSchemaConverter.toSchema(hCatSchema);
    } else {
      schema = config.getSchema();
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
