/*
 * Copyright © 2015 Cask Data, Inc.
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

/**
 * Constants for ElasticSearch plugins.
 */
public class ESProperties {
  public static final String INDEX_NAME = "es.index";
  public static final String TYPE_NAME = "es.type";
  public static final String HOST = "es.host";
  public static final String QUERY = "query";
  public static final String SCHEMA = "schema";

  public static final String ID_FIELD = "es.idField";
  public static final String TRANSPORT_ADDRESSES = "es.transportAddresses";
  public static final String CLUSTER = "es.cluster";

  private ESProperties() {
  }
}
