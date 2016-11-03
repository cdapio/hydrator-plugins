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
package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.hydrator.plugin.common.SolrSearchSinkConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Realtime SolrSearch Sink Plugin - Writes data to a SingleNode Solr or to SolrCloud.
 */
@Plugin(type = "realtimesink")
@Name("SolrSearch")
@Description("This plugin allows users to build the pipelines to write data to Solr. The input fields coming from " +
  "the previous stage of the pipeline are mapped to Solr fields. User can also specify the mode of the Solr to " +
  "connect to. For example, SingleNode Solr or SolrCloud.")
public class RealtimeSolrSearchSink extends RealtimeSink<StructuredRecord> {
  private final SolrSearchSinkConfig config;
  private String keyField;
  private Map<String, String> outputFieldMap;
  private SolrClient solrClient;
  private StageMetrics metrics;

  public RealtimeSolrSearchSink(SolrSearchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validateSolrConnectionString();
    if (inputSchema != null) {
      config.validateKeyField(inputSchema);
      config.validateInputFieldsDataType(inputSchema);
    }
    config.validateOutputFieldMappings();
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    keyField = config.getKeyField();
    solrClient = config.getSolrConnection();
    outputFieldMap = config.createOutputFieldMap();
    metrics = context.getMetrics();
    //Calling testSolrConnection() before each mapper, to ensure that the connection is alive and available for
    //indexing.
    config.testSolrConnection();

  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;
    List<SolrInputDocument> documentList = new ArrayList<SolrInputDocument>();

    for (StructuredRecord structuredRecord : structuredRecords) {
      config.validateKeyField(structuredRecord.getSchema());
      config.validateInputFieldsDataType(structuredRecord.getSchema());

      if (structuredRecord.get(keyField) == null) {
        metrics.count("invalid", 1);
        continue;
      }
      SolrInputDocument document = new SolrInputDocument();
      for (Schema.Field field : structuredRecord.getSchema().getFields()) {
        String solrFieldName = field.getName();
        if (outputFieldMap.containsKey(solrFieldName)) {
          document.addField(outputFieldMap.get(solrFieldName), structuredRecord.get(solrFieldName));
        } else {
          document.addField(solrFieldName, structuredRecord.get(solrFieldName));
        }
      }
      documentList.add(document);
      numRecordsWritten++;
    }
    solrClient.add(documentList);
    solrClient.commit();
    return numRecordsWritten;
  }

  @Override
  public void destroy() {
    solrClient.shutdown();
  }
}
