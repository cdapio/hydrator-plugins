/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.db.common;

import io.cdap.plugin.common.db.DBUtils;
import io.opentracing.contrib.jdbc.ConnectionInfo;
import io.opentracing.contrib.jdbc.parser.URLParser;

/**
 * Generate FQN from DB URL Connection
 */

public class FQNGenerator {

  public static String constructFQN(String url, String tableName) {
    // dbtype, host, port, db from the connection string
    // table is the reference name
    ConnectionInfo connectionInfo = URLParser.parse(url);
    // DB type as set by library after extraction
    if (DBUtils.POSTGRESQL_TAG.equals(connectionInfo.getDbType())) {
      // FQN for Postgresql
      return String.format("%s://%s/%s.%s.%s", connectionInfo.getDbType(), connectionInfo.getDbPeer(),
                           connectionInfo.getDbInstance(), getPostgresqlSchema(url), tableName);
    } else {
      // FQN for MySQL, Oracle, SQLServer
      return String.format("%s://%s/%s.%s", connectionInfo.getDbType(), connectionInfo.getDbPeer(),
                           connectionInfo.getDbInstance(), tableName);
    }
  }

  private static String getPostgresqlSchema(String url) {
    /**
     * Extract schema for PostgresSQL URL strings which can be of the following formats
     * jdbc:postgresql://{host}:{port}/{db}?currentSchema={schema}
     * jdbc:postgresql://{host}:{port}/{db}?searchpath={schema}
     */
    String dbSchema;
    int offset =  0;
    int startIndex = url.indexOf("currentSchema=");
    offset = 14;
    if (startIndex == -1) {
      startIndex = url.indexOf("searchpath=");
      offset = 11;
    }

    int endIndex = url.indexOf("&", startIndex);
    if (endIndex == -1) {
      endIndex = url.length();
    }
    if (startIndex != -1 && endIndex != -1 && startIndex <= endIndex) {
      dbSchema = url.substring(startIndex + offset, endIndex);
    } else {
      dbSchema = DBUtils.POSTGRESQL_DEFAULT_SCHEMA;
    }
    return dbSchema;
  }
}
