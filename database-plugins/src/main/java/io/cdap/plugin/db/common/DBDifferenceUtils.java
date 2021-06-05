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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
   * Replace the database name in the JDBC connection string with specified database name
   *
   * @param connectionString the JDBC connection string
   * @param database         the database name
   * @return the replaced JDBC connection string
   */
  public static String replaceDatabase(String connectionString, @Nullable String database) {
    if (database == null) {
      return connectionString;
    }
    //SQLServer connection string is special, it's arguments are separated by ";" :
    // https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url
    if (connectionString.startsWith("jdbc:sqlserver")) {
      int databaseStartIndex = connectionString.indexOf(";" + SQLSERVER_DATABSE_PARAM);
      if (databaseStartIndex < 0) {
        connectionString = connectionString.endsWith(";") ? connectionString : connectionString + ";";
        return connectionString + SQLSERVER_DATABSE_PARAM + database + ";";
      } else {
        databaseStartIndex += SQLSERVER_DATABSE_PARAM.length() + 1;
        int databaseEndIndex = connectionString.indexOf(";", databaseStartIndex);
        if (databaseEndIndex < 0) {
          databaseEndIndex = connectionString.length();
        }
        return connectionString.substring(0, databaseStartIndex) + database +
          connectionString.substring(databaseEndIndex);
      }
    } else {
      // find the separator between host and port
      int lastColonIndex = connectionString.lastIndexOf(":");
      int lastSlashIndex = connectionString.lastIndexOf("/");
      // arguments starting with "?"
      int lastQuestionIndex = connectionString.lastIndexOf("?");
      // in normal case database part is between the last slash and last question mark:
      // e.g. jdbc:postgresql://localhost:5432/database?param=v&param=2
      int databaseEndIndex = lastQuestionIndex < 0 ? connectionString.length() : lastQuestionIndex;
      // if last slash is before last colon, that means there is no database info in the connection string
      // e.g. jdbc:postgresql://localhost:5432
      int databaseStartIndex = lastSlashIndex < lastColonIndex ? databaseEndIndex : lastSlashIndex;
      return connectionString.substring(0, databaseStartIndex) + "/" + database +
        connectionString.substring(databaseEndIndex);
    }
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

  /**
   * Get the list of databases
   * @param connection the connection to the database
   * @return the result set of list of databases
   */
  public static ResultSet getDatabases(Connection connection) throws SQLException {
    String productName = connection.getMetaData().getDatabaseProductName().trim().toUpperCase();
    switch (productName) {
      case DB_PRODUCT_NAME_ORACLE:
        // Oracle JDBC will return empty result for metaData.getCatalogs()
        // because Oracle only supports one database per instance.
        // from Oracle 12c , oracle introduced CDB/PDB concepts
        // ref : https://oracle-base.com/articles/12c/multitenant-overview-container-database-cdb-12cr1
        // we don't support list PDBs for now.
        return connection.createStatement().executeQuery(getOracleDatabasesQuery());
      case DB_PRODUCT_NAME_POSTGRESQL:
        //PostgreSQL does not support multiple catalogs from a single connection
        //metaData.getCatalogs only return the current catalog. So need to handle it differently
        return connection.createStatement().executeQuery(getPostgreDatabasesQuery());
      default:
        return connection.getMetaData().getCatalogs();
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
