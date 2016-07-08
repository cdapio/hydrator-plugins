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
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.hydrator.plugin.common.SolrSearchSinkConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Realtime SolrSearch Sink Plugin - Writes data to a Sinlge Node Solr or to SolrCloud.
 */
@Plugin(type = "realtimesink")
@Name("SolrSearch")
@Description("This plugin allows users to build the pipelines to write data to Solr. " +
  "The input fields coming from the previous stage of the pipeline are mapped to Solr fields. User can also specify " +
  "the mode of the Solr to connect to. For example, Single Node Solr or SolrCloud.")
public class RealtimeSolrSearchSink extends RealtimeSink<StructuredRecord> {

  private static final String SINGLE_NODE_SOLR_MODE = "Single Node Solr";
  private static final String SOLR_CLOUD_MODE = "SolrCloud";
  private final SolrSearchSinkConfig config;
  private String uniqueKey;
  private Map<String, String> outputFieldMappings;
  private SolrClient solrClient;

  public RealtimeSolrSearchSink(SolrSearchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    validateSolrConnectionString();
    verifySolrConfiguration();
    if (inputSchema != null) {
      validateIdField(inputSchema);
      validateInputFieldsDataType(inputSchema);
    }
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    uniqueKey = config.getIdField();
    outputFieldMappings = config.getOutputFieldMappings();
    solrClient = getSolrConnection();
  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;
    String solrFieldName;
    List<SolrInputDocument> documentList = new ArrayList<SolrInputDocument>();
    SolrInputDocument document;

    for (StructuredRecord structuredRecord : structuredRecords) {
      validateIdField(structuredRecord.getSchema());
      validateInputFieldsDataType(structuredRecord.getSchema());

      if (structuredRecord.get(uniqueKey) == null) {
        continue;
      }
      document = new SolrInputDocument();
      for (Schema.Field field : structuredRecord.getSchema().getFields()) {
        solrFieldName = field.getName();
        if (outputFieldMappings.containsKey(solrFieldName)) {
          document.addField(outputFieldMappings.get(solrFieldName), structuredRecord.get(solrFieldName));
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

  /**
   * Returns the SolrClient for establishing the connection with Solr server, using the hostname and port provided by
   * user.
   *
   * @return Solr client
   */
  private SolrClient getSolrConnection() {
    String urlString;
    SolrClient solrClient = null;
    if (config.getSolrMode().equals(SINGLE_NODE_SOLR_MODE)) {
      urlString = "http://" + config.getSolrHost() + "/solr/" + config.getCollectionName();
      solrClient = new HttpSolrClient(urlString);
    } else if (config.getSolrMode().equals(SOLR_CLOUD_MODE)) {
      CloudSolrClient solrCloudClient = new CloudSolrClient(config.getSolrHost());
      solrCloudClient.setDefaultCollection(config.getCollectionName());
      solrClient = solrCloudClient;
    }
    return solrClient;
  }

  /**
   * Validates whether the host entered for Single Node Solr instance is proper or not.
   */
  private void validateSolrConnectionString() {
    if (config.getSolrMode().equals(SINGLE_NODE_SOLR_MODE) && config.getSolrHost().contains(",")) {
      throw new IllegalArgumentException(String.format("Multiple hosts '%s' found for Single Node Solr.",
                                                       config.getSolrHost()));
    }
  }

  /**
   * Verifies whether the Solr server is running or not. Also, validates whether the collection provided by user is
   * registered with the Solr server or not.
   */
  private void verifySolrConfiguration() {
    SolrClient client = getSolrConnection();
    try {
      client.ping();
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Server refused connection at '%s'. Please make sure that either the Solr/Zookeeper services " +
                        "are properly running or the collection '%s' exists in the Solr Server.", config.getSolrHost(),
                      config.getCollectionName()));
    }
  }

  /**
   * Validates whether the Id field provided by user is present in the input schema or not.
   *
   * @param inputSchema
   */
  private void validateIdField(Schema inputSchema) {
    if (inputSchema != null && inputSchema.getField(config.getIdField()) == null) {
      throw new IllegalArgumentException(
        String.format("Idfield '%s' does not exist in the input schema %s", config.getIdField(), inputSchema));
    }
  }

  /**
   * Validates whether the CDAP data types coming from the input schema are compatible with the Solr data types or not.
   *
   * @param inputSchema
   */
  private void validateInputFieldsDataType(Schema inputSchema) {
    /* Currently SolrSearch sink plugin supports CDAP primitives types only: BOOLEAN, INT, LONG, FLOAT, DOUBLE and
    STRING.*/
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        switch (field.getSchema().getType()) {
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            break;
          default:
            throw new IllegalArgumentException(
              String.format("Data type '%s' is not compatible for writing data to the Solr Server. Supported CDAP " +
                              "data types are ' BOOLEAN, INT, LONG, FLOAT, DOUBLE and STRING '",
                            field.getSchema().getType()));
        }
      }
    }
  }
}
