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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Common schema reader for mapping non specific DB types.
 */
public class CommonSchemaReader implements SchemaReader {

  @Override
  public List<Schema.Field> getSchemaFields(ResultSet resultSet,
                                            String regexPattern,
                                            String replacement) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if (shouldIgnoreColumn(metadata, i)) {
        continue;
      }
      String columnName = metadata.getColumnName(i);
      if (!Strings.isNullOrEmpty(regexPattern)) {
        columnName = columnName.replaceAll(regexPattern,
                Strings.isNullOrEmpty(replacement) ? "" : replacement);
      }
      Schema columnSchema = getSchema(metadata, i);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  /**
   * Extending classes should not override this method.
   *
   * @param metadata resultSet metadata
   * @param index column index
   * @return
   * @throws SQLException
   */
  @Override
  public final Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int precision = metadata.getPrecision(index);
    int scale = metadata.getScale(index);
    String columnName = metadata.getColumnName(index);
    int sqlType = metadata.getColumnType(index);
    String sqlTypeName = metadata.getColumnTypeName(index);
    String columnClassName = metadata.getColumnClassName(index);
    boolean isSigned = metadata.isSigned(index);

    return getSchema(columnName, sqlType, sqlTypeName, columnClassName, precision, scale, isSigned);
  }

  @Override
  public Schema getSchema(String columnName, int sqlType, String sqlTypeName, String columnClassName,
                          int precision, int scale, boolean isSigned) throws SQLException {

    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.ROWID:
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
        type = Schema.Type.INT;
        break;
      case Types.INTEGER:
        // SQL INT is 32 bit, thus only signed can be stored in int
        type = isSigned ? Schema.Type.INT : Schema.Type.LONG;
        break;

      case Types.BIGINT:
        //SQL BIGINT is 64 bit, thus signed can be stored in long without losing precision
        //or unsigned BIGINT is within the scope of signed long
        if (isSigned || precision < 19) {
          type = Schema.Type.LONG;
          break;
        } else {
          // by default scale is 0, big integer won't have any fraction part
          return Schema.decimalOf(precision);
        }

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        return Schema.decimalOf(precision, scale);

      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException(
                String.format("Column %s has unsupported SQL type of %s.", columnName, sqlType)));
    }

    return Schema.of(type);
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    return false;
  }
}
