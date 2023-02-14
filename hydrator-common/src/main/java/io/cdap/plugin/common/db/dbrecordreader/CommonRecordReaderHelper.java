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
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.annotation.Nullable;

/**
 * Writable class for DB Source/Sink
 *
 * @see org.apache.hadoop.mapreduce.lib.db.DBInputFormat DBInputFormat
 * @see org.apache.hadoop.mapreduce.lib.db.DBOutputFormat DBOutputFormat
 * @see DBWritable DBWritable
 */
public class CommonRecordReaderHelper implements RecordReader {

  protected long bytesRead;

  private static final Calendar PURE_GREGORIAN_CALENDAR = createPureGregorianCalender();

  // Java by default uses October 15, 1582 as a Gregorian cut over date.
  // Any timestamp created with time less than this cut over date is treated as Julian date.
  // This causes old dates from database such as 0001-01-01 01:00:00 mapped to 0000-12-30
  // Get the pure gregorian calendar so that all dates are treated as gregorian format.
  private static Calendar createPureGregorianCalender() {
    GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    gc.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
    return gc;
  }

  /**
   * Returns the bytes read during the previous readRecord
   * @return Bytes read for a single record
   */
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Builds the {@link StructuredRecord} using the specified {@link ResultSet} and {@link Schema}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public StructuredRecord.Builder getRecordBuilder(ResultSet resultSet, Schema schema) throws SQLException {
    bytesRead = 0;
    ResultSetMetaData metadata = resultSet.getMetaData();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getFields().get(i);
      int columnIndex = i + 1;
      int sqlType = metadata.getColumnType(columnIndex);
      int sqlPrecision = metadata.getPrecision(columnIndex);
      int sqlScale = metadata.getScale(columnIndex);

      handleField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
    return recordBuilder;
  }

  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }

  protected void setField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                          int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    Object value = transformValue(resultSet, sqlType, sqlPrecision, sqlScale, columnIndex, field.getSchema());
    if (value instanceof Date) {
      bytesRead += Long.BYTES;
      recordBuilder.setDate(field.getName(), ((Date) value).toLocalDate());
    } else if (value instanceof Time) {
      bytesRead += Integer.BYTES;
      recordBuilder.setTime(field.getName(), ((Time) value).toLocalTime());
    } else if (value instanceof Timestamp) {
      bytesRead += Long.BYTES;
      Instant instant = ((Timestamp) value).toInstant();
      recordBuilder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
    } else if (value instanceof BigDecimal) {
      BigDecimal decimal = (BigDecimal) value;
      bytesRead += decimal.unscaledValue().bitLength() / Byte.SIZE + Integer.BYTES;
      recordBuilder.setDecimal(field.getName(), decimal);
    } else if (value instanceof BigInteger) {
      Schema schema = field.getSchema();
      schema = schema.isNullable() ? schema.getNonNullable() : schema;
      if (schema.getType() == Schema.Type.LONG) {
        bytesRead += Long.BYTES;
        recordBuilder.set(field.getName(), ((BigInteger) value).longValueExact());
      } else {
        // BigInteger won't have any fraction part and scale is 0
        BigDecimal decimal = new BigDecimal((BigInteger) value, 0);
        bytesRead += decimal.unscaledValue().bitLength() / Byte.SIZE + Integer.BYTES;
        recordBuilder.setDecimal(field.getName(), decimal);
      }
    } else {
      if (value != null) {
        Schema schema = field.getSchema();
        if (schema.isNullable()) {
          schema = schema.getNonNullable();
        }
        switch (schema.getType()) {
          case INT:
          case BOOLEAN:
            bytesRead += Integer.BYTES;
            break;
          case LONG:
            bytesRead += Long.BYTES;
            break;
          case DOUBLE:
            bytesRead += Double.BYTES;
            break;
          case FLOAT:
            bytesRead += Float.BYTES;
            break;
          case STRING:
            String strVal = (String) value;
            //make sure value is in the right format for datetime
            if (schema.getLogicalType() == Schema.LogicalType.DATETIME) {
              try {
                LocalDateTime.parse(strVal);
              } catch (DateTimeParseException exception) {
                throw new UnexpectedFormatException(
                        String.format("Datetime field '%s' with value '%s' is not in ISO-8601 format.", field.getName(),
                                strVal), exception);
              }
            }
            bytesRead += strVal.length();
            break;
          case BYTES:
            bytesRead += ((byte[]) value).length;
            break;
        }
      }
      recordBuilder.set(field.getName(), value);
    }
  }

  @Nullable
  private Object transformValue(ResultSet resultSet,
                                int sqlType,
                                int precision,
                                int scale,
                                int columnIndex,
                                Schema outputFieldSchema) throws SQLException {
    Object result = resultSet.getObject(columnIndex);
    if (result != null) {
      switch (sqlType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) result).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL: {
          if (Schema.LogicalType.DECIMAL == outputFieldSchema.getLogicalType()) {
            // It's required to pass 'scale' parameter since in the case of some dbs like Oracle,
            //  scale of 'BigDecimal' depends on the scale of actual value. For example for value '77.12'
            // scale will be '2' even if sql scale is '6'
            return resultSet.getBigDecimal(columnIndex, scale);
          } else if (Schema.Type.STRING == outputFieldSchema.getType()) {
            return resultSet.getString(columnIndex);
          } else {
            BigDecimal decimal = (BigDecimal) result;
            if (scale != 0) {
              // if there are digits after the point, use double types
              return decimal.doubleValue();
            } else if (precision > 9) {
              // with 10 digits we can represent 2^32 and LONG is required
              return decimal.longValue();
            } else {
              return decimal.intValue();
            }
          }
        }
        case Types.DATE:
          return resultSet.getDate(columnIndex);
        case Types.TIME:
          return resultSet.getTime(columnIndex);
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(columnIndex, PURE_GREGORIAN_CALENDAR);
        case Types.ROWID:
          return resultSet.getString(columnIndex);
        case Types.BLOB:
          Blob blob = (Blob) result;
          return blob.getBytes(1, (int) blob.length());
        case Types.CLOB:
        case Types.NCLOB:
          Clob clob = (Clob) result;
          return clob.getSubString(1, (int) clob.length());
      }
    }

    return result;
  }


  protected void setFieldAccordingToSchema(ResultSet resultSet, StructuredRecord.Builder recordBuilder,
                                           Schema.Field field, int columnIndex) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    switch (fieldType) {
      case NULL:
        break;
      case STRING:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case BOOLEAN:
        recordBuilder.set(field.getName(), resultSet.getBoolean(columnIndex));
        break;
      case INT:
        recordBuilder.set(field.getName(), resultSet.getInt(columnIndex));
        break;
      case LONG:
        recordBuilder.set(field.getName(), resultSet.getLong(columnIndex));
        break;
      case FLOAT:
        recordBuilder.set(field.getName(), resultSet.getFloat(columnIndex));
        break;
      case DOUBLE:
        recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        break;
      case BYTES:
        recordBuilder.set(field.getName(), resultSet.getBytes(columnIndex));
        break;
    }
  }
}
