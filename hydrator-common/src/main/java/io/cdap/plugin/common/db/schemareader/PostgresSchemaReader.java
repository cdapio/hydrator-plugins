/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.common.db.schemareader;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * PostgreSQL schema reader.
 */
public class PostgresSchemaReader extends CommonSchemaReader {

  public static final Set<Integer> STRING_MAPPED_POSTGRES_TYPES = ImmutableSet.of(
    Types.OTHER, Types.ARRAY, Types.SQLXML
  );

  public static final Set<String> STRING_MAPPED_POSTGRES_TYPES_NAMES = ImmutableSet.of(
    "bit", "timetz", "money"
  );

  private final String sessionID;

  public PostgresSchemaReader() {
    this(null);
  }

  public PostgresSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(String columnName, int sqlType, String sqlTypeName, String columnClassName,
                          int precision, int scale, boolean isSigned) throws SQLException {
    if (STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(sqlTypeName) || STRING_MAPPED_POSTGRES_TYPES.contains(sqlType)) {
      return Schema.of(Schema.Type.STRING);
    }

    return super.getSchema(columnName, sqlType, sqlTypeName, columnClassName, precision, scale, isSigned);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("sqn_" + sessionID);
  }
}
