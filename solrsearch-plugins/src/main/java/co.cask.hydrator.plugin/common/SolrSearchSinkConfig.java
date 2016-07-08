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
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for SolrSearchSink and RealtimeSolrSearchSink plugins.
 */
public class SolrSearchSinkConfig extends ReferencePluginConfig {

  @Description("Solr mode to connect to. For example, Single Node Solr or SolrCloud.")
  private final String solrMode;

  @Description("The hostname and port for the Solr server seperated by colon. For example, localhost:8983 for " +
    "Single Node Solr or comma-seperated list of hostname and port, zkHost1:2181,zkHost2:2181,zkHost3:2181 " +
    "for SolrCloud.")
  private final String solrHost;

  @Description("Name of the collection where data will be indexed and stored in Solr.")
  private final String collectionName;

  @Description("Field that will determine the unique id for the document to be indexed. It should match a " +
    "fieldname in the structured record of the input.")
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
   * Returns the Solr mode selected as input by user.
   *
   * @return Solr mode
   */
  public String getSolrMode() {
    return solrMode;
  }

  /**
   * Returns the hostname and port entered by user.
   *
   * @return Solr host
   */
  public String getSolrHost() {
    return solrHost;
  }

  /**
   * Returns the Solr collection name given as input by user.
   *
   * @return collection name
   */
  public String getCollectionName() {
    return collectionName;
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
  public Map<String, String> getOutputFieldMappings() {
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
}
