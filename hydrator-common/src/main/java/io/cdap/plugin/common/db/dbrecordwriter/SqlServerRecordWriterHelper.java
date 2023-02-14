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

package io.cdap.plugin.common.db.dbrecordwriter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.schemareader.SqlServerSourceSchemaReader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import javax.annotation.Nullable;

/**
 * SQL Server specific DB Writer
 */
public class SqlServerRecordWriterHelper extends CommonRecordWriterHelper {

    @Override
    protected void writeToDB(PreparedStatement stmt,
                             StructuredRecord record,
                             @Nullable Schema.Field field,
                             int fieldIndex,
                             ColumnType columnType) throws SQLException {
        Object fieldValue = (field != null) ? record.get(field.getName()) : null;
        int sqlType = columnType.getType();
        int sqlIndex = fieldIndex + 1;
        switch (sqlType) {
            case SqlServerSourceSchemaReader.GEOGRAPHY_TYPE:
            case SqlServerSourceSchemaReader.GEOMETRY_TYPE:
                if (fieldValue == null) {
                    // Handle setting GEOGRAPHY and GEOMETRY 'null' values.
                    // Using 'stmt.setNull(sqlIndex, GEOMETRY_TYPE)' leads to
                    // "com.microsoft.sqlserver.jdbc.SQLServerException:
                    // The conversion from OBJECT to GEOMETRY is unsupported"
                    stmt.setString(sqlIndex, "Null");
                } else if (fieldValue instanceof String) {
                    // Handle setting GEOGRAPHY and GEOMETRY values from Well Known Text.
                    // For example, "POINT(3 40 5 6)"
                    stmt.setString(sqlIndex, (String) fieldValue);
                } else {
                    super.writeBytes(stmt, sqlIndex, fieldValue, columnType);
                }
                break;
            case Types.TIME:
                // Handle setting SQL Server 'TIME' data type as string to avoid accuracy loss.
                // 'TIME' data type has the accuracy of 100 nanoseconds(1 millisecond in Informatica)
                // but 'java.sql.Time' will round value to second.
                if (fieldValue != null) {
                    String fieldName = field.getName();
                    stmt.setString(sqlIndex, record.getTime(fieldName).toString());
                } else {
                    stmt.setNull(sqlIndex, sqlType);
                }
                break;
            default:
                super.writeToDB(stmt, record, field, fieldIndex, columnType);
        }
    }
}
