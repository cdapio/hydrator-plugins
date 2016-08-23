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

package co.cask.hydrator.plugin.batch.source;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from a BigQuery table
 */
@co.cask.cdap.api.annotation.Plugin(type = "batchsource")
@Name("BigQuerySource")
@Description("Reads from a database table(s) using a configurable BigQuery." +
  " Outputs one record for each row returned by the query.")
public class BigQuerySource extends ReferenceBatchSource<LongWritable, JsonObject, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  private final BQSourceConfig sourceConfig;
  private static final String MRBQ_JSON_KEY = "mapred.bq.auth.service.account.json.keyfile";
  private static final String FSGS_JSON_KEY = "google.cloud.auth.service.account.json.keyfile";
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("title", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("unique_words", Schema.of(Schema.Type.INT))
  );
  private Map<String, String> outputSchemaMapping = new HashMap<>();
  private Schema outputSchema;
  public BigQuerySource(BQSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.sourceConfig = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    init();
  }

  private void init() {
    String[] schemaList = sourceConfig.outputSchema.split(",");
    for (String schema : schemaList) {
      String[] columns = schema.split(":");
      outputSchemaMapping.put(columns[0], columns[1]);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    init();
    getOutputSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException, GeneralSecurityException,
    ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance();
    int id = 1;
    JobID jobID = new JobID("bigquery", id);
    job.setJobID(jobID);
    Configuration conf = job.getConfiguration();
    JobConf jobConf = new JobConf(conf, BigQuerySource.class);
    jobConf.set(MRBQ_JSON_KEY, sourceConfig.jsonFilePath);
    jobConf.set(FSGS_JSON_KEY, sourceConfig.jsonFilePath);
    jobConf.set(BigQueryConfiguration.PROJECT_ID_KEY, sourceConfig.projectId);
    if (sourceConfig.importQuery != null) {
      jobConf.set(BigQueryConfiguration.INPUT_QUERY_KEY, sourceConfig.importQuery);
    }
    if (sourceConfig.bucketKey != null) {
      jobConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, sourceConfig.bucketKey);
    }
    if (sourceConfig.tmpBucketPath != null) {
      jobConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, sourceConfig.tmpBucketPath);
    }
    jobConf.set("fs.gs.project.id", sourceConfig.projectId);
    BigQueryConfiguration.configureBigQueryInput(jobConf, sourceConfig.fullyQualifiedInputTableId);
    job.setOutputKeyClass(LongWritable.class);
    jobConf.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(JsonObject.class);
    job.setJarByClass(BigQuerySource.class);
    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    LOG.debug("output Key class is {}", jobConf.getPropertySources(MRJobConfig.OUTPUT_KEY_CLASS));
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(JsonObject.class);
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(GsonBigQueryInputFormat.class, jobConf)));
//    job.waitForCompletion(true);
//    GsonBigQueryInputFormat.cleanupJob(job);
  }


  @Override
  public void transform(KeyValue<LongWritable, JsonObject> input, Emitter<StructuredRecord> emitter) {
//    getOutputSchema();
//    LOG.debug("input is {}", input.getValue());
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, null);
    }
    emitter.emit(builder.build());
  }

  private StructuredRecord jsonTransform(JsonObject jsonObject) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, jsonObject.get(fieldName));
    }
    return builder.build();
  }

  /**
   * {@link PluginConfig} class for {@link BigQuerySource}
   */
  public static class BQSourceConfig extends PluginConfig {
    public static final String IMPORT_QUERY = "importQuery";
    public static final String PROJECT_ID = "projectId";
    public static final String INPUT_TABLE_ID = "input_tableId";
    public static final String JSON_FILE_PATH = "json_file_path";
    public static final String BUCKET_KEY = "bucket_key";
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Name(IMPORT_QUERY)
    @Description("The SELECT query to use to import data from the specified table. ")
    @Nullable
    @Macro
    String importQuery;

    @Name(PROJECT_ID)
    @Description("The ID of the project in Google Cloud")
    @Macro
    String projectId;

    @Name("outputSchema")
    @Description("Comma separated mapping of column names in the output schema to the data types;" +
      "for example: 'A:string,B:int'. This input is required if no inputs " +
      "for 'columnList' has been provided.")
    @Macro
    private String outputSchema;

    @Name(INPUT_TABLE_ID)
    @Description("The BigQuery table to read from, in the form [optional projectId]:[datasetId].[tableId]." +
      " Example: publicdata:samples.shakespeare")
    @Macro
    String fullyQualifiedInputTableId;

    @Name(JSON_FILE_PATH)
    @Description("the credential json key file path")
    @Macro
    String jsonFilePath;

    @Name(BUCKET_KEY)
    @Description("the bucket connector uses")
    @Nullable
    @Macro
    String bucketKey;

    @Name("tmp bucket path")
    @Description("GCS temp path this connector uses")
    @Nullable
    @Macro
    String tmpBucketPath;
  }

  private void getOutputSchema() {
    if (outputSchema == null) {
      List<Schema.Field> outputFields = Lists.newArrayList();
      try {
        for (String fieldName : outputSchemaMapping.keySet()) {
          String columnName = fieldName;
          Schema fieldType = Schema.of(Schema.Type.valueOf(outputSchemaMapping.get(fieldName).toUpperCase()));
          outputFields.add(Schema.Field.of(columnName, fieldType));
        }

      } catch (Exception e) {
        throw new IllegalArgumentException("Exception while creating output schema " +
                                             "Invalid output " + "schema: " + e.getMessage(), e);
      }
      outputSchema = Schema.recordOf("outputSchema", outputFields);
    }
  }

}
