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

package co.cask.hydrator.plugin.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
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
import co.cask.hydrator.common.batch.JobUtils;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BigQuerySource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("BigQuery")
@Description("Reads from a database table(s) using a configurable BigQuery." +
  " Outputs one record for each row returned by the query.")
public class BigQuerySource extends ReferenceBatchSource<LongWritable, Text, StructuredRecord> {
  private final BQSourceConfig sourceConfig;
  private static final String MRBQ_JSON_KEY = "mapred.bq.auth.service.account.json.keyfile";
  private static final String FSGS_JSON_KEY = "google.cloud.auth.service.account.json.keyfile";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private Map<String, String> outputSchemaMapping = new HashMap<>();
  private  Schema outputSchema;
  public BigQuerySource(BQSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.sourceConfig = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
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
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    conf.set(MRBQ_JSON_KEY, sourceConfig.jsonFilePath);
    conf.set(FSGS_JSON_KEY, sourceConfig.jsonFilePath);
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, sourceConfig.projectId);
    if (sourceConfig.importQuery != null) {
      conf.set(BigQueryConfiguration.INPUT_QUERY_KEY, sourceConfig.importQuery);
    }
      conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, sourceConfig.tmpBucketPath);
    conf.set("fs.gs.project.id", sourceConfig.projectId);
    BigQueryConfiguration.configureBigQueryInput(conf, sourceConfig.fullyQualifiedInputTableId);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(JsonTextBigQueryInputFormat.class, conf)));
  }


  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws IOException {
    getOutputSchema();
    StructuredRecord record = jsonTransform(input.getValue());
    emitter.emit(record);

  }

  private StructuredRecord jsonTransform(Text jsonText) {
    Map<String, String> map;
    map = GSON.fromJson(jsonText.toString(), MAP_STRING_STRING_TYPE);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      Schema schema = field.getSchema();
      if (schema.getType() == Schema.Type.INT) {
        builder.set(fieldName, Integer.parseInt(map.get(fieldName)));
      } else {
        builder.set(fieldName, map.get(fieldName));
      }
    }
    return builder.build();
  }

  /**
   * {@link PluginConfig} class for {@link BigQuerySource}
   */
  public static class BQSourceConfig extends PluginConfig {
    private static final String IMPORT_QUERY = "importQuery";
    private static final String PROJECT_ID = "projectId";
    private static final String INPUT_TABLE_ID = "InputTableId";
    private static final String JSON_FILE_PATH = "jsonFilePath";
    private static final String TEMP_BUCKET = "tempBuketPath";
    private static final String OUTPUT_SCHEMA = "outputSchema";
    private static final String PROJECTID_DESC = "The ID of the project in Google Cloud";
    private static final String TEMP_BUCKET_DESC = "the tempory google cloud storage directory to store the " +
                                                   "intermediate result. Example: gs://bucketname/directoryname, " +
                                                   "the directory should not be existed. User should delete this " +
                                                   "directory afterward manually to avoid extra google storage charge.";
    private static final String IMPORT_QUERY_DESC = "The SELECT query to use to import data from the specified table." +
                                                    " example: SELECT TOP(corpus, 10) as title, COUNT(*) as " +
                                                    "unique_words FROM [publicdata:samples.shakespeare], where " +
                                                    "publicdata is the project name, smalples is the dataset name, " +
                                                    "shakespare is the table name. This is optional, if empty, just " +
                                                    "read the inputTable configured";
    private static final String JSON_KEYFILE_DESC = "the credential json key file path";
    private static final String INPUT_TABLE_DESC = "The BigQuery table to read from, in the form " +
                                                   "[optional projectId]:[datasetId].[tableId]. Example: " +
                                                   "publicdata:samples.shakespeare. Note that if the import query is " +
                                                   "specified, this table should be a empty table with the " +
                                                   "query result schema. User need to first create such a table";
    private static final String OUTPUTSCHEMA_DESC = "Comma separated mapping of column names in the output schema to " +
                                                   "the data types; for example: 'A:string,B:int'";
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Name(IMPORT_QUERY)
    @Description(IMPORT_QUERY_DESC)
    @Nullable
    @Macro
    String importQuery;

    @Name(PROJECT_ID)
    @Description(PROJECTID_DESC)
    @Macro
    String projectId;

    @Name(OUTPUT_SCHEMA)
    @Description(OUTPUTSCHEMA_DESC)
    @Macro
    private String outputSchema;

    @Name(INPUT_TABLE_ID)
    @Description(INPUT_TABLE_DESC)
    @Macro
    String fullyQualifiedInputTableId;

    @Name(JSON_FILE_PATH)
    @Description(JSON_KEYFILE_DESC)
    @Macro
    String jsonFilePath;

    @Name(TEMP_BUCKET)
    @Description(TEMP_BUCKET_DESC)
    @Macro
    String tmpBucketPath;
  }

  private void getOutputSchema() {
    if (outputSchema == null) {
      List<Schema.Field> outputFields = Lists.newArrayList();
      try {
        for (String fieldName : outputSchemaMapping.keySet()) {
          Schema fieldType = Schema.of(Schema.Type.valueOf(outputSchemaMapping.get(fieldName).toUpperCase()));
          outputFields.add(Schema.Field.of(fieldName, fieldType));
        }

      } catch (Exception e) {
        throw new IllegalArgumentException("Exception while creating output schema " +
                                             "Invalid output " + "schema: " + e.getMessage(), e);
      }
      outputSchema = Schema.recordOf("outputSchema", outputFields);
    }
  }

}
