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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.db.schemareader.OracleSourceSchemaReader;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * Oracle DB specific record writer.
 */
public class OracleRecordWriterHelper extends CommonRecordWriterHelper {

    @Override
    protected void writeToDB(PreparedStatement stmt,
                             StructuredRecord record,
                             @Nullable Schema.Field field,
                             int fieldIndex,
                             ColumnType columnType) throws SQLException {
        int sqlType = columnType.getType();
        int sqlIndex = fieldIndex + 1;
        switch (sqlType) {
            case OracleSourceSchemaReader.TIMESTAMP_TZ:
                if (field != null && record.get(field.getName()) != null) {
                    // Set value of Oracle 'TIMESTAMP WITH TIME ZONE' data type as instance of 'oracle.sql.TIMESTAMPTZ',
                    // created from timestamp string, such as "2019-07-15 15:57:46.65 GMT".
                    String timestampString = record.get(field.getName());
                    Object timestampWithTimeZone =
                            createOracleTimestampWithTimeZone(stmt.getConnection(), timestampString);
                    stmt.setObject(sqlIndex, timestampWithTimeZone);
                } else {
                    stmt.setNull(sqlIndex, sqlType);
                }
                break;
            default:
                super.writeToDB(stmt, record, field, fieldIndex, columnType);
        }
    }

    /**
     * Creates an instance of 'oracle.sql.TIMESTAMPTZ' which corresponds
     * to the specified timestamp with time zone string.
     * @param connection sql connection.
     * @param timestampString timestamp with time zone string, such as "2019-07-15 15:57:46.65 GMT".
     * @return instance of 'oracle.sql.TIMESTAMPTZ' which corresponds to the specified timestamp with time zone string.
     */
    private Object createOracleTimestampWithTimeZone(Connection connection, String timestampString) {
        try {
            ClassLoader classLoader = connection.getClass().getClassLoader();
            Class<?> timestampTZClass = classLoader.loadClass("oracle.sql.TIMESTAMPTZ");
            return timestampTZClass.
                    getConstructor(Connection.class, String.class).
                    newInstance(connection, timestampString);
        } catch (ClassNotFoundException e) {
            throw new InvalidStageException("Unable to load 'oracle.sql.TIMESTAMPTZ'.", e);
        } catch (InstantiationException
                 | InvocationTargetException
                 | NoSuchMethodException
                 | IllegalAccessException e) {
            throw new InvalidStageException("Unable to instantiate 'oracle.sql.TIMESTAMPTZ'.", e);
        }
    }

    @Override
    protected int writeBytes(PreparedStatement stmt,
                              int sqlIndex,
                              Object fieldValue,
                              ColumnType columnType) throws SQLException {
        byte[] byteValue =
                fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
        // handles BINARY, VARBINARY and LOGVARBINARY
        stmt.setBytes(sqlIndex, byteValue);
        return byteValue.length;
    }
}
