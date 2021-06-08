/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * A DB Util that handles the difference of different DB
 */
public final class DBDifferenceUtils {
  public static final String DB_PRODUCT_NAME_POSTGRESQL = "POSTGRESQL";
  public static final String DB_PRODUCT_NAME_ORACLE = "ORACLE";
  public static final String DB_PRODUCT_NAME_SQLSERVER = "MICROSOFT SQL SERVER";
  public static final String RESULTSET_COLUMN_TABLE_CAT = "TABLE_CAT";
  private static final String SQLSERVER_DATABSE_PARAM = "databaseName=";

  private DBDifferenceUtils() {
  }

  /**
   * Generates SQL query on the specified table with limit of number of returned rows for specific database
   * @param productName the product name of the database
   * @param schema the schema of the table
   * @param table the name of the table
   * @param limit the limit of number returned rows
   * @return generated SQL query
   */
  public static String getTableQueryWithLimit(String productName, @Nullable String schema, String table, int limit) {
    productName = productName.trim().toUpperCase();
    switch (productName) {
      case DB_PRODUCT_NAME_ORACLE:
        // Oracle DB doesn't support LIMIT, have to use ROWNUM
        return String.format("SELECT * FROM %s.%s WHERE ROWNUM = %d", schema, table, limit);
      case DB_PRODUCT_NAME_SQLSERVER:
        // SQL Server doesn't support LIMIT, have to use TOP
        return String.format("SELECT TOP(%d) * FROM %s.%s", limit, schema, table);
      default:
        return schema == null ? String.format("SELECT * FROM %s LIMIT %d", table, limit) :
          String.format("SELECT * FROM %s.%s LIMIT %d", schema, table, limit);
    }
  }

  @VisibleForTesting
  static String getPostgreDatabasesQuery() {
    return String
      .format("SELECT datname AS %s FROM pg_database WHERE datistemplate = false;", RESULTSET_COLUMN_TABLE_CAT);
  }

  @VisibleForTesting
  static String getOracleDatabasesQuery() {
    return String.format("SELECT NAME AS %s FROM V$DATABASE", RESULTSET_COLUMN_TABLE_CAT);
  }
}
