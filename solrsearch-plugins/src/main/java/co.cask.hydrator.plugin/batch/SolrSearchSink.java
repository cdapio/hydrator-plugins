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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.common.SolrSearchSinkConfig;
import co.cask.hydrator.plugin.common.SolrSearchSinkUtility;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.util.Map;

/**
 * Batch SolrSearch Sink Plugin - Writes data to a Sinlge Node Solr or to SolrCloud.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SolrSearch")
@Description("This plugin allows users to build the pipelines to write data to Solr. " +
  "The input fields coming from the previous stage of the pipeline are mapped to Solr fields. User can also specify " +
  "the mode of the Solr to connect to. For example, Single Node Solr or SolrCloud.")
public class SolrSearchSink extends BatchSink<StructuredRecord, SolrInputField, SolrInputDocument> {

  private final SolrSearchSinkConfig config;
  private String uniqueKey;
  private Map<String, String> outputFieldMap;
  private SolrClient solrClient;

  public SolrSearchSink(SolrSearchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    // Setting outputformat class in order to resolve the 'Output directory not set' error.
    Job job = batchSinkContext.getHadoopJob();
    job.setOutputFormatClass(NullOutputFormat.class);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    SolrSearchSinkUtility.validateSolrConnectionString(config);
    SolrSearchSinkUtility.verifySolrConfiguration(config);
    if (inputSchema != null) {
      SolrSearchSinkUtility.validateIdField(inputSchema, config);
      SolrSearchSinkUtility.validateInputFieldsDataType(inputSchema);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    uniqueKey = config.getIdField();
    outputFieldMap = config.getOutputFieldMap();
    solrClient = SolrSearchSinkUtility.getSolrConnection(config);
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<KeyValue<SolrInputField, SolrInputDocument>> emitter)
    throws Exception {
    String solrFieldName;
    SolrInputDocument document;

    SolrSearchSinkUtility.validateIdField(structuredRecord.getSchema(), config);
    SolrSearchSinkUtility.validateInputFieldsDataType(structuredRecord.getSchema());

    if (structuredRecord.get(uniqueKey) == null) {
      return;
    }
    document = new SolrInputDocument();
    for (Schema.Field field : structuredRecord.getSchema().getFields()) {
      solrFieldName = field.getName();
      if (outputFieldMap.containsKey(solrFieldName)) {
        document.addField(outputFieldMap.get(solrFieldName), structuredRecord.get(solrFieldName));
      } else {
        document.addField(solrFieldName, structuredRecord.get(solrFieldName));
      }
    }
    solrClient.add(document);
    solrClient.commit();
    emitter.emit(new KeyValue<>(document.getField(uniqueKey), document));
  }

  @Override
  public void destroy() {
    solrClient.shutdown();
  }
}
