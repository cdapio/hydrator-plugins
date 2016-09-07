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
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.lang.reflect.Type;
import java.security.GeneralSecurityException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BigQuerySource} that reads from Google BigQuery.
 */
@Plugin(type = "batchsource")
@Name("BigQuery")
@Description("Reads from BigQuery tables specified by a configurable BigQuery. " +
  "Outputs one record for each row returned by the query.")
public class BigQuerySource extends ReferenceBatchSource<LongWritable, Text, StructuredRecord> {
  private static final String MAPREDUCE_BIGQUERY_JSON_KEY = "mapred.bq.auth.service.account.json.keyfile";
  private static final String GCS_ACCOUNT_JSON_KEY = "google.cloud.auth.service.account.json.keyfile";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final BQSourceConfig sourceConfig;

  public BigQuerySource(BQSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.sourceConfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    sourceConfig.validate();
    if (!Strings.isNullOrEmpty(sourceConfig.outputSchema)) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(sourceConfig.getSchema());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException, GeneralSecurityException,
    ClassNotFoundException, InterruptedException {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(MAPREDUCE_BIGQUERY_JSON_KEY, sourceConfig.jsonKeyFilePath);
    conf.set(GCS_ACCOUNT_JSON_KEY, sourceConfig.jsonKeyFilePath);
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, sourceConfig.projectId);
    if (sourceConfig.importQuery != null) {
      conf.set(BigQueryConfiguration.INPUT_QUERY_KEY, sourceConfig.importQuery);
    }
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, sourceConfig.tmpBucketPath);
    conf.set("fs.gs.project.id", sourceConfig.projectId);
    BigQueryConfiguration.configureBigQueryInput(conf, sourceConfig.tableToRead);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(JsonTextBigQueryInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws IOException {
    StructuredRecord record = convertToStructuredRecord(input.getValue());
    emitter.emit(record);
  }

  private StructuredRecord convertToStructuredRecord(Text jsonText) {
    Map<String, String> map = GSON.fromJson(jsonText.toString(), MAP_STRING_STRING_TYPE);
    Schema outputSchema = sourceConfig.getSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      Schema schema = field.getSchema();
      Schema.Type fieldType = schema.getType();
      switch (fieldType) {
        case INT:
          builder.set(fieldName, Integer.parseInt(map.get(fieldName)));
          break;
        case BYTES:
          builder.set(fieldName, Byte.parseByte(map.get(fieldName)));
          break;
        case LONG:
          builder.set(fieldName, Long.parseLong(map.get(fieldName)));
          break;
        case FLOAT:
          builder.set(fieldName, Float.parseFloat(map.get(fieldName)));
          break;
        case DOUBLE:
          builder.set(fieldName, Double.parseDouble(map.get(fieldName)));
          break;
        case BOOLEAN:
          builder.set(fieldName, Boolean.parseBoolean(map.get(fieldName)));
          break;
        case NULL:
          builder.set(fieldName, null);
          break;
        case STRING:
          builder.set(fieldName, map.get(fieldName));
          break;
        default:
          throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
      }
    }
    return builder.build();
  }

  /**
   * {@link PluginConfig} class for {@link BigQuerySource}
   */
  public static class BQSourceConfig extends PluginConfig {
    public static String tableToRead;
    private static final String PROJECTID_DESC = "The ID of the project in Google Cloud.";
    private static final String TEMP_BUCKET_DESC = "The temporary Google Storage directory to be used for storing " +
                                                   "the intermediate results. For example: " +
                                                   "'gs://bucketname/directoryname'. The directory should not " +
                                                   "already exist. Users should manually delete this directory " +
                                                   "afterwards to avoid any extra Google Storage charges.";
    private static final String IMPORT_QUERY_DESC = "The SELECT query to use to import data from the specified " +
                                                    "table. For example: 'SELECT TOP(corpus, 10) as title, COUNT(*) " +
                                                    "as unique_words FROM publicdata:samples.shakespeare', where " +
                                                    "'publicdata' is the project name (optional), 'samples' is the " +
                                                    "dataset name, and 'shakespare' is the table name. If this query " +
                                                    "is not provided, it instead reads from the configured inputTable.";
    private static final String JSON_KEYFILE_DESC = "JSON key file path for credentials, Note that user should " +
                                                    "upload his credential file to the same path of all nodes.";
    private static final String INPUT_TABLE_DESC = "The BigQuery table to read from, in the form " +
                                                   "'<projectId (optional)>:<datasetId>.<tableId>'. The 'projectId' " +
                                                   "is optional. Example: 'myproject:mydataset.mytable'. Note: if " +
                                                   "the import query is provided, then this property should be left " +
                                                   "empty. If the import query is not provided, " +
                                                   "BigQuery source will read the content from the inputTable.";
    private static final String OUTPUTSCHEMA_DESC = "Comma-separated mapping of output schema column names to " +
                                                    "data types; for example: 'A:string,B:int'.";
    private static final String INTERMEDIATE_TABLE_DESC = "An intermediate table to store the query result, in the " +
                                                          "form '<projectId (optional)>:<datasetId>.<tableId>'. The" +
                                                          "'projectId' is optional. Example: 'myproject:mydataset." +
                                                          "mytable'. Note: if the import query is provided, this " +
                                                          "table should be provided, it should be a blank table with " +
                                                          "the query result schema so as to store the intermediate " +
                                                          "result. If the import query is not provided, this " +
                                                          "property should be left empty.";
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    String referenceName;

    @Name("importQuery")
    @Description(IMPORT_QUERY_DESC)
    @Nullable
    @Macro
    String importQuery;

    @Name("projectId")
    @Description(PROJECTID_DESC)
    @Macro
    String projectId;

    @Name("outputSchema")
    @Description(OUTPUTSCHEMA_DESC)
    private String outputSchema;

    @Name("inputTable")
    @Description(INPUT_TABLE_DESC)
    @Nullable
    @Macro
    String inputTable;

    @Name("jsonKeyFilePath")
    @Description(JSON_KEYFILE_DESC)
    @Macro
    String jsonKeyFilePath;

    @Name("tempBucketPath")
    @Description(TEMP_BUCKET_DESC)
    @Macro
    String tmpBucketPath;

    @Name("intermediateTable")
    @Nullable
    @Description(INTERMEDIATE_TABLE_DESC)
    String intermediateTable;

    private Schema getSchema() {
      try {
        return Schema.parseJson(outputSchema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                         outputSchema, e.getMessage()), e);
      }
    }

    private  void validate() {
      Preconditions.checkArgument(tmpBucketPath.startsWith("gs://"), "Google Storage Path should start with 'gs://'");
      if (importQuery != null) {
        Preconditions.checkArgument(intermediateTable != null && inputTable == null,
                                    "If import query is provided, intermediateTable should be provided while " +
                                     "inputTable should not");
        tableToRead = intermediateTable;
      } else {
        Preconditions.checkArgument(intermediateTable == null && inputTable != null,
                                    "If import query is not provided, inputTable should be provided while " +
                                    "intermediateTable should not");
        tableToRead = inputTable;
      }
    }
  }
}
