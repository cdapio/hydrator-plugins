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
import io.cdap.plugin.common.db.schemareader.PostgresSchemaReader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * PostgreSQL specific DB Writer.
 */
public class PostgresRecordWriterHelper extends CommonRecordWriterHelper {

    @Override
    protected void writeToDB(PreparedStatement stmt,
                             StructuredRecord record,
                             Schema.Field field,
                             int fieldIndex,
                             ColumnType columnType) throws SQLException {
        int sqlIndex = fieldIndex + 1;
        if (PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES_NAMES.contains(columnType.getTypeName()) ||
                PostgresSchemaReader.STRING_MAPPED_POSTGRES_TYPES.contains(columnType.getType())) {
            stmt.setObject(sqlIndex, createPGobject(columnType.getTypeName(),
                    record.get(field.getName()),
                    stmt.getClass().getClassLoader()));
        } else {
            super.writeToDB(stmt, record, field, fieldIndex, columnType);
        }
    }

    private Object createPGobject(String type, String value, ClassLoader classLoader) throws SQLException {
        try {
            Class pGObjectClass = classLoader.loadClass("org.postgresql.util.PGobject");
            Method setTypeMethod = pGObjectClass.getMethod("setType", String.class);
            Method setValueMethod = pGObjectClass.getMethod("setValue", String.class);
            Object result = pGObjectClass.newInstance();
            setTypeMethod.invoke(result, type);
            setValueMethod.invoke(result, value);
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new SQLException("Failed to create instance of org.postgresql.util.PGobject");
        }
    }
}
