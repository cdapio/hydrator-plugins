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
package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.plugin.common.SolrOutputFormat;
import co.cask.hydrator.plugin.common.SolrRecordWriter;
import co.cask.hydrator.plugin.common.SolrSearchSinkConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.Text;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Batch SolrSearch Sink Plugin - Writes data to a Sinlge Node Solr or to SolrCloud.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SolrSearch")
@Description("This plugin allows users to build the pipelines to write data to Solr. " +
  "The input fields coming from the previous stage of the pipeline are mapped to Solr fields. User can also specify " +
  "the mode of the Solr to connect to. For example, Single Node Solr or SolrCloud.")
public class SolrSearchSink extends BatchSink<StructuredRecord, Text, Text> {
  private static final Gson GSON = new Gson();
  private static final Type SCHEMA_TYPE = new TypeToken<Schema>() { }.getType();
  private final BatchSolrSearchConfig batchConfig;

  public SolrSearchSink(BatchSolrSearchConfig batchConfig) {
    this.batchConfig = batchConfig;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(batchConfig.referenceName, new SolrSearchSink.SolrOutputFormatProvider(batchConfig)));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    batchConfig.validateSolrConnectionString();
    if (inputSchema != null) {
      batchConfig.validateKeyField(inputSchema);
      batchConfig.validateInputFieldsDataType(inputSchema);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    batchConfig.verifySolrConfiguration();
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<KeyValue<Text, Text>> emitter) throws Exception {
    batchConfig.validateKeyField(structuredRecord.getSchema());
    batchConfig.validateInputFieldsDataType(structuredRecord.getSchema());

    if (structuredRecord.get(batchConfig.getKeyField()) == null) {
      return;
    }
    emitter.emit(new KeyValue<Text, Text>(new Text(GSON.toJson(structuredRecord.getSchema(), SCHEMA_TYPE)),
                                          new Text(StructuredRecordStringConverter.toJsonString(structuredRecord))));
  }

  /**
   * Output format provider for Batch SolrSearch Sink.
   */
  private static class SolrOutputFormatProvider implements OutputFormatProvider {
    private Map<String, String> conf;

    SolrOutputFormatProvider(BatchSolrSearchConfig batchConfig) {
      this.conf = new HashMap<>();
      conf.put(SolrRecordWriter.SERVER_URL, batchConfig.getSolrHost());
      conf.put(SolrRecordWriter.SERVER_MODE, batchConfig.getSolrMode());
      conf.put(SolrRecordWriter.COLLECTION_NAME, batchConfig.getCollectionName());
      conf.put(SolrRecordWriter.KEY_FIELD, batchConfig.getKeyField());
      conf.put(SolrRecordWriter.BATCH_SIZE, batchConfig.getBatchSize());
      if (batchConfig.getOutputFieldMappings() == null) {
        conf.put(SolrRecordWriter.FIELD_MAPPINGS, "");
      } else {
        conf.put(SolrRecordWriter.FIELD_MAPPINGS, batchConfig.getOutputFieldMappings());
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return SolrOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config class for Batch SolrSearch sink.
   */
  public static class BatchSolrSearchConfig extends SolrSearchSinkConfig {
    @Description("Number of documents to create a batch and send it to Solr for indexing. After each batch, " +
      "commit will be triggered. Default batch size is 10000.")
    private final String batchSize;

    public BatchSolrSearchConfig(String referenceName, String solrMode, String solrHost, String collectionName,
                                 String keyField, String outputFieldMappings, String batchSize) {
      super(referenceName, solrMode, solrHost, collectionName, keyField, outputFieldMappings);
      this.batchSize = batchSize;
    }

    /**
     * Returns the batch size given as input by user.
     *
     * @return batch size
     */
    public String getBatchSize() {
      return batchSize;
    }
  }
}
