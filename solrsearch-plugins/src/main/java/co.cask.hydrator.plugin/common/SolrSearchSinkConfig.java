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
package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for SolrSearchSink and RealtimeSolrSearchSink plugins.
 */
public class SolrSearchSinkConfig extends ReferencePluginConfig {

  public static final String SINGLE_NODE_MODE = "SingleNode";
  public static final String SOLR_CLOUD_MODE = "SolrCloud";

  @Description("Solr mode to connect to. For example, Single Node Solr or SolrCloud.")
  private final String solrMode;
  @Description("The hostname and port for the Solr server separated by a colon. For example, localhost:8983 for " +
    "Single Node Solr or comma-separated list of hostname and port, zkHost1:2181,zkHost2:2181,zkHost3:2181 " +
    "for SolrCloud.")
  private final String solrHost;
  @Description("Name of the collection where data will be indexed and stored in Solr.")
  private final String collectionName;
  @Description("Field that will determine the unique id for the document to be indexed. It must match a " +
    "field name in the structured record of the input.")
  private final String idField;
  @Description("List of the input fields to map to the output Solr fields. The key specifies the name of the field to" +
    " rename, with its corresponding value specifying the new name for that field.")
  @Nullable
  private final String outputFieldMappings;

  public SolrSearchSinkConfig(String referenceName, String solrMode, String solrHost, String collectionName,
                              String idField, String outputFieldMappings) {
    super(referenceName);
    this.solrMode = solrMode;
    this.solrHost = solrHost;
    this.collectionName = collectionName;
    this.idField = idField;
    this.outputFieldMappings = outputFieldMappings;
  }

  /**
   * Return the Id field given as input by user.
   *
   * @return id field
   */
  public String getIdField() {
    return idField;
  }

  /**
   * Creates a map for input field and its mapping for output field, if present.
   *
   * @return Map of input fields and its mapping
   */
  public Map<String, String> getOutputFieldMap() {
    Map<String, String> outputFieldMap = new HashMap<String, String>();
    if (StringUtils.isNotEmpty(outputFieldMappings)) {
      for (String field : Splitter.on(',').trimResults().split(outputFieldMappings)) {
        String[] value = field.split(":");
        if (value.length == 2) {
          outputFieldMap.put(value[0], value[1]);
        } else {
          throw new IllegalArgumentException(
            String.format("Either key or value is missing for 'Fields to rename'. Please make sure that " +
                            "both key and value are present."));
        }
      }
    }
    return outputFieldMap;
  }

  /**
   * Returns the SolrClient for establishing the connection with Solr server, using the hostname and port provided by
   * user.
   *
   * @return Solr client
   */
  public SolrClient getSolrConnection() {
    String urlString;
    SolrClient solrClient = null;
    if (solrMode.equalsIgnoreCase(SINGLE_NODE_MODE)) {
      urlString = "http://" + solrHost + "/solr/" + collectionName;
      solrClient = new HttpSolrClient(urlString);
    } else if (solrMode.equalsIgnoreCase(SOLR_CLOUD_MODE)) {
      CloudSolrClient solrCloudClient = new CloudSolrClient(solrHost);
      solrCloudClient.setDefaultCollection(collectionName);
      solrClient = solrCloudClient;
    }
    return solrClient;
  }

  /**
   * Validates whether the host entered for Single Node Solr instance is proper or not.
   */
  public void validateSolrConnectionString() {
    if (solrMode.equals(SINGLE_NODE_MODE) && solrHost.contains(",")) {
      throw new IllegalArgumentException(String.format("Multiple hosts '%s' found for Single Node Solr.",
                                                       solrHost));
    }
  }

  /**
   * Verifies whether the Solr server is running or not. Also, validates whether the collection provided by user is
   * registered with the Solr server or not.
   */
  public void verifySolrConfiguration() {
    SolrClient client = getSolrConnection();
    try {
      client.ping();
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Server refused connection at '%s'. Please make sure that either the Solr/Zookeeper services " +
                        "are properly running or the collection '%s' exists in the Solr Server.", solrHost,
                      collectionName));
    }
  }

  /**
   * Validates whether the Id field provided by user is present in the input schema or not.
   *
   * @param inputSchema
   */
  public void validateIdField(Schema inputSchema) {
    if (inputSchema != null && inputSchema.getField(idField) == null) {
      throw new IllegalArgumentException(
        String.format("Idfield '%s' does not exist in the input schema %s", idField, inputSchema));
    }
  }

  /**
   * Validates whether the CDAP data types coming from the input schema are compatible with the Solr data types or not.
   *
   * @param inputSchema
   */
  public void validateInputFieldsDataType(Schema inputSchema) {
    /* Currently SolrSearch sink plugin supports CDAP primitives types only: BOOLEAN, INT, LONG, FLOAT, DOUBLE and
    STRING.*/
    Schema.Type schemaType;
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        schemaType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() :
          field.getSchema().getType();

        switch (schemaType) {
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case NULL:
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
