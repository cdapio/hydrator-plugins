/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.plugin.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
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

  @Description("Solr mode to connect to. For example, SingleNode Solr or SolrCloud.")
  private final String solrMode;
  @Description("The hostname and port for the Solr server separated by a colon. For example, localhost:8983 for " +
    "SingleNode Solr or comma-separated list of hostname and port, zkHost1:2181,zkHost2:2181,zkHost3:2181 " +
    "for SolrCloud.")
  private final String solrHost;
  @Description("Name of the collection where data will be indexed and stored in Solr.")
  private final String collectionName;
  @Description("Field that will determine the unique key for the document to be indexed. It must match a " +
    "field name in the structured record of the input.")
  private final String keyField;
  @Description("List of the input fields to map to the output Solr fields. This is a comma-separated list of " +
    "key-value pairs, where each pair is separated by a colon ':' and specifies the input and output names. For " +
    "example, 'firstname:fname,lastname:lname' specifies that the 'firstname' should be renamed to 'fname' and the " +
    "'lastname' should be renamed to 'lname'.")
  @Nullable
  private final String outputFieldMappings;

  public SolrSearchSinkConfig(String referenceName, String solrMode, String solrHost, String collectionName,
                              String keyField, @Nullable String outputFieldMappings) {
    super(referenceName);
    this.solrMode = solrMode;
    this.solrHost = solrHost;
    this.collectionName = collectionName;
    this.keyField = keyField;
    this.outputFieldMappings = outputFieldMappings;
  }

  /**
   * Returns the Solr mode.
   *
   * @return Solr mode
   */
  public String getSolrMode() {
    return solrMode;
  }

  /**
   * Returns the hostname and port.
   *
   * @return Solr host
   */
  public String getSolrHost() {
    return solrHost;
  }

  /**
   * Returns the Solr collection name.
   *
   * @return collection name
   */
  public String getCollectionName() {
    return collectionName;
  }

  /**
   * Returns the Key field.
   *
   * @return key field
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * Returns the input fields and its output fields mapping.
   *
   * @return
   */
  @Nullable
  public String getOutputFieldMappings() {
    return outputFieldMappings;
  }

  /**
   * Creates a map for input field and its mapping for output field, if present.
   * If mapping is not present, then empty map will be returned.
   *
   * @return Map of input fields and its mapping or empty
   */
  public Map<String, String> createOutputFieldMap() {
    Map<String, String> outputFieldMap = new HashMap<String, String>();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(outputFieldMappings)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(outputFieldMappings)) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        outputFieldMap.put(key, val);
      }
    }
    return outputFieldMap;
  }

  /**
   * Returns the SolrClient for establishing the connection with Solr server, using the hostname and port.
   *
   * @return Solr client
   */
  public SolrClient getSolrConnection() {
    String urlString;
    SolrClient solrClient = null;
    if (solrMode.equals(SINGLE_NODE_MODE)) {
      urlString = "http://" + solrHost + "/solr/" + collectionName;
      solrClient = new HttpSolrClient(urlString);
    } else if (solrMode.equals(SOLR_CLOUD_MODE)) {
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
   * Verifies whether the Solr server is running or not. Also, validates whether the collection is registered with
   * the Solr server or not.
   */
  public void testSolrConnection() {
    SolrClient client = getSolrConnection();
    try {
      client.ping();
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Server refused connection at '%s'. Please make sure that either the Solr/Zookeeper services " +
                        "are properly running or the collection '%s' exists in the Solr Server.", solrHost,
                      collectionName), e);
    }
  }

  /**
   * Validates whether the key field is present in the input schema or not.
   *
   * @param inputSchema
   */
  public void validateKeyField(Schema inputSchema) {
    if (inputSchema != null && inputSchema.getField(keyField) == null) {
      throw new IllegalArgumentException(
        String.format("Key field '%s' does not exist in the input schema %s", keyField, inputSchema));
    }
  }

  /**
   * Validates whether the CDAP data types coming from the input schema are compatible with the Solr data types or not.
   *
   * @param inputSchema
   */
  public void validateInputFieldsDataType(Schema inputSchema) {
    // Currently SolrSearch sink plugin supports CDAP primitives types only: BOOLEAN, INT, LONG, FLOAT, DOUBLE and
    //STRING.
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
                              "data types are ' BOOLEAN, INT, LONG, FLOAT, DOUBLE and STRING '.",
                            field.getSchema().getType()));
        }
      }
    }
  }

  /**
   * Validates whether the key or value for key-value pair that specifies input and output names, is present or not.
   */
  public void validateOutputFieldMappings() {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(outputFieldMappings)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(outputFieldMappings)) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(val)) {
          throw new IllegalArgumentException(
            String.format("Expected field and its rename field has to be separated by ':'. For example, " +
                            "oldFieldName:newFieldName, but received '%s'. Please make sure that both key and value " +
                            "are present for 'Fields to rename'.", outputFieldMappings));
        }
      }
    }
  }
}
