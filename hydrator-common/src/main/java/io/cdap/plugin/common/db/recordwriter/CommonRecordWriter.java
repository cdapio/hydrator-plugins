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

package io.cdap.plugin.common.db.recordwriter;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import javax.annotation.Nullable;
import javax.sql.rowset.serial.SerialBlob;

/**
 * Record Writer Helper class which provides the methods to write the StructuredRecord.
 */
public class CommonRecordWriter implements RecordWriter {

    protected long bytesWritten;

    public void write(DataOutput out, StructuredRecord record) throws IOException {
        Schema recordSchema = record.getSchema();
        List<Schema.Field> schemaFields = recordSchema.getFields();
        for (Schema.Field field : schemaFields) {
            writeToDataOut(out, record, field);
        }
    }

    /**
     * Writes the {@link StructuredRecord} to the specified {@link PreparedStatement}
     *
     * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
     */
    public void write(PreparedStatement stmt,
                      StructuredRecord record,
                      List<ColumnType> columnTypes) throws SQLException {
        bytesWritten = 0;
        for (int i = 0; i < columnTypes.size(); i++) {
            ColumnType columnType = columnTypes.get(i);
            Schema.Field field = record.getSchema().getField(columnType.getName());
            writeToDB(stmt, record, field, i, columnType);
        }
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten;
    }

    private Schema getNonNullableSchema(Schema.Field field) {
        Schema schema = field.getSchema();
        if (field.getSchema().isNullable()) {
            schema = field.getSchema().getNonNullable();
        }
        Preconditions.checkArgument(schema.getType().isSimpleType(),
                "Only simple types are supported (boolean, int, long, float, double, string, bytes) " +
                        "for writing a DBRecord, but found '%s' as the type for column '%s'. Please " +
                        "remove this column or transform it to a simple type.", schema.getType(),
                field.getName());
        return schema;
    }

    private void writeToDataOut(DataOutput out, StructuredRecord record, Schema.Field field) throws IOException {
        Schema fieldSchema = getNonNullableSchema(field);
        Schema.Type fieldType = fieldSchema.getType();
        Object fieldValue = record.get(field.getName());

        if (fieldValue == null) {
            return;
        }

        switch (fieldType) {
            case NULL:
                break;
            case STRING:
                // write string appropriately
                out.writeUTF((String) fieldValue);
                break;
            case BOOLEAN:
                out.writeBoolean((Boolean) fieldValue);
                break;
            case INT:
                // write short or int appropriately
                out.writeInt((Integer) fieldValue);
                break;
            case LONG:
                // write date, timestamp or long appropriately
                out.writeLong((Long) fieldValue);
                break;
            case FLOAT:
                // both real and float are set with the same method on prepared statement
                out.writeFloat((Float) fieldValue);
                break;
            case DOUBLE:
                out.writeDouble((Double) fieldValue);
                break;
            case BYTES:
                out.write((byte[]) fieldValue);
                break;
            default:
                throw new IOException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
        }
    }

    protected void writeToDB(PreparedStatement stmt,
                             StructuredRecord record,
                             @Nullable Schema.Field field,
                             int fieldIndex,
                             ColumnType columnType) throws SQLException {
        String fieldName = field.getName();
        Schema fieldSchema = getNonNullableSchema(field);
        Schema.Type fieldType = fieldSchema.getType();
        Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
        Object fieldValue = record.get(fieldName);
        int sqlIndex = fieldIndex + 1;

        if (fieldValue == null) {
            stmt.setNull(sqlIndex, columnType.getType());
            return;
        }

        if (fieldLogicalType != null) {
            switch (fieldLogicalType) {
                case DATE:
                    stmt.setDate(sqlIndex, Date.valueOf(record.getDate(fieldName)));
                    bytesWritten += Long.BYTES;
                    break;
                case TIME_MILLIS:
                    stmt.setTime(sqlIndex, Time.valueOf(record.getTime(fieldName)));
                    bytesWritten += Integer.BYTES;
                    break;
                case TIME_MICROS:
                    stmt.setTime(sqlIndex, Time.valueOf(record.getTime(fieldName)));
                    bytesWritten += Long.BYTES;
                    break;
                case TIMESTAMP_MILLIS:
                case TIMESTAMP_MICROS:
                    stmt.setTimestamp(sqlIndex, Timestamp.from(record.getTimestamp(fieldName).toInstant()));
                    bytesWritten += Long.BYTES;
                    break;
                case DECIMAL:
                    BigDecimal value = record.getDecimal(fieldName);
                    stmt.setBigDecimal(sqlIndex, value);
                    bytesWritten += value.unscaledValue().bitLength() / Byte.SIZE + Integer.BYTES;
                    break;
                case DATETIME:
                    stmt.setString(sqlIndex, (String) fieldValue);
                    bytesWritten += ((String) fieldValue).length();
                    break;
            }
            return;
        }

        switch (fieldType) {
            case NULL:
                stmt.setNull(sqlIndex, columnType.getType());
                break;
            case STRING:
                // clob can also be written to as setString
                stmt.setString(sqlIndex, (String) fieldValue);
                bytesWritten += ((String) fieldValue).length();
                break;
            case BOOLEAN:
                stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
                bytesWritten += Integer.BYTES;
                break;
            case INT:
                // write short or int appropriately
                writeInt(stmt, sqlIndex, fieldValue, columnType);
                bytesWritten += Integer.BYTES;
                break;
            case LONG:
                stmt.setLong(sqlIndex, (Long) fieldValue);
                bytesWritten += Long.BYTES;
                break;
            case FLOAT:
                // both real and float are set with the same method on prepared statement
                stmt.setFloat(sqlIndex, (Float) fieldValue);
                bytesWritten += Float.BYTES;
                break;
            case DOUBLE:
                stmt.setDouble(sqlIndex, (Double) fieldValue);
                bytesWritten += Double.BYTES;
                break;
            case BYTES:
                bytesWritten += writeBytes(stmt, sqlIndex, fieldValue, columnType);
                break;
            default:
                throw new SQLException(String.format("Column %s with value %s has an unsupported datatype %s",
                        field.getName(), fieldValue, fieldType));
        }
    }

    protected int writeBytes(PreparedStatement stmt,
                           int sqlIndex,
                           Object fieldValue,
                           ColumnType columnType) throws SQLException {
        byte[] byteValue =
                fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
        int parameterType = columnType.getType();
        if (Types.BLOB == parameterType) {
            stmt.setBlob(sqlIndex, new SerialBlob(byteValue));
        } else {
            // handles BINARY, VARBINARY and LOGVARBINARY
            stmt.setBytes(sqlIndex, byteValue);
        }
        return byteValue.length;
    }

    protected void writeInt(PreparedStatement stmt,
                            int sqlIndex,
                            Object fieldValue,
                            ColumnType columnType) throws SQLException {
        Integer intValue = (Integer) fieldValue;
        int parameterType = columnType.getType();
        if (Types.TINYINT == parameterType || Types.SMALLINT == parameterType) {
            stmt.setShort(sqlIndex, intValue.shortValue());
            return;
        }
        stmt.setInt(sqlIndex, intValue);
    }
}
