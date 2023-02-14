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

package io.cdap.plugin.common.db.dbrecordreader;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.schemareader.PostgresSchemaReader;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Writable class for PostgreSQL Source/Sink
 */
public class PostgresRecordReaderHelper extends CommonRecordReaderHelper {

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public PostgresRecordReaderHelper() {
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (isUseSchema(resultSet.getMetaData(), columnIndex)) {
      setFieldAccordingToSchema(resultSet, recordBuilder, field, columnIndex);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private static boolean isUseSchema(ResultSetMetaData metadata, int columnIndex) throws SQLException {
    switch (metadata.getColumnTypeName(columnIndex)) {
      case "bit":
      case "timetz":
      case "money":
        return true;
      default:
        return PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(metadata.getColumnType(columnIndex));
    }
  }
}
