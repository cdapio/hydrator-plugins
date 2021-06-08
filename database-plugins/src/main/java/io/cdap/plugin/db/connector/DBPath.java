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

package io.cdap.plugin.db.connector;

import javax.annotation.Nullable;

/**
 * BigQuery Path that parses the path in the request of connection service
 * A valid path can start with/without a slash, e.g. "/" , "", "/database/schema/table", "database/schema/table" are
 * all valid.
 * A valid path can end with/without a slash, e.g. "/", "", "/database/schema/table/", "database/schema/table/" are all
 * valid.
 * A valid path should contain at most two parts for those DB that don't support schema (e.g. mysql) or three parts
 * for those DB that support schema (e.g. postgresql, oracle, sqlserver) separated by slash. The first part is
 * database name. The second part is schema name for those DB that support schema or table name for those DB that
 * don't support schema. Third part is table name (only applicable for those DB that support schema). Any part is
 * optional. e.g. "/" , "/database" , "/database/schema" (for those DB that support Schema), "database/schema/table"
 * (for those DB that support Schema), "/database/table" (for those DB that don't support Schema)
 * Consecutive slashes are not valid , it will be parsed as there is an empty string part between the slashes. e.g.
 * "//a" will be parsed as database name as empty and schema name or table name as "a". Similarly "/a//" will be parsed
 * as database name as "a" and table name or schema name as empty.
 *
 */
public class DBPath {
  private final boolean supportSchema;
  private String schema;
  private String table;

  public DBPath(String path, boolean supportSchema) {
    this.supportSchema = supportSchema;
    parsePath(path);
  }

  private void parsePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path should not be null.");
    }

    //remove heading "/" if exists
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // both "" and "/" are taken as root path
    if (path.isEmpty()) {
      return;
    }

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    String[] parts = path.split("/", -1);

    // path should contain at most two parts : schema and table
    // for those that don't support schema, path should contain at most 1 part : table
    if (parts.length > 2 || !supportSchema && parts.length == 2) {
      throw new IllegalArgumentException(String.format("Path should not contain more than %d parts.",
        supportSchema ? 2 : 1));
    }

    if (parts.length > 0) {
      if (supportSchema) {
        schema = parts[0];
        validateName("Schema", schema);
      } else {
        table = parts[0];
        validateName("Table", table);
      }

      if (parts.length == 2) {
        table = parts[1];
        validateName("Table", table);
      }
    }
  }

  private void validateName(String property, String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("%s should not be empty.", property));
    }
  }

  @Nullable
  public String getTable() {
    return table;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  public boolean supportSchema() {
    return this.supportSchema;
  }
}
