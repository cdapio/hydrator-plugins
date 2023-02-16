/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.DBRecord;
import io.cdap.plugin.common.db.DBUtils;
import io.cdap.plugin.common.db.recordwriter.ColumnType;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.sql.rowset.serial.SerialBlob;

public class DBRecordTest {

  @Test
  public void testDBRecordWrite() throws Exception {
    Long expectedLong = 12435353L;
    int longSize = Long.BYTES;
    Float expectedFloat = 13.33f;
    int floatSize = Float.BYTES;
    Double expectedDouble = 3.14159;
    int doubleSize = Double.BYTES;
    String expectedString = "dasfadf";
    int stringSize = expectedString.length();
    LocalTime expectedTime = LocalTime.now();
    int timeSize = Long.BYTES;
    Integer expectedInteger = 123;
    int integerSize = Integer.BYTES;
    ZonedDateTime expectedMillis = ZonedDateTime.of(2018, 11, 11, 11, 11,
                                                    11, 123 * 1000 * 1000,
                                                    ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    int timeMillisSize = Integer.BYTES;
    ZonedDateTime expectedMicros = ZonedDateTime.of(2018, 11, 11, 11, 11,
                                                    11, 123456 * 1000,
                                                    ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    int timeMicrosSize = Long.BYTES;
    byte[] expectedBlob = new byte[]{1, 2, 3, 4};
    int blobSize = expectedBlob.length;
    byte[] expectedBinary = new byte[]{0, 1};
    int binarySize = expectedBinary.length;
    boolean expectedBoolean = true;
    int booleanSize = Integer.BYTES;
    LocalDateTime localDateTime = LocalDateTime.now();
    String formatedDt = localDateTime.format(DateTimeFormatter.ISO_DATE_TIME);


    long expectedBytesWritten = longSize + floatSize + doubleSize + stringSize + timeSize * 2 + integerSize +
      timeMillisSize + timeMicrosSize + blobSize + binarySize + booleanSize + formatedDt.length();
    StructuredRecord input = StructuredRecord
      .builder(Schema.recordOf(
        "foo",
        Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
        Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
        Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("timestamp_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
        Schema.Field.of("timestamp_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
        Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
        Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
        Schema.Field.of("int", Schema.of(Schema.Type.INT)),
        Schema.Field.of("blob", Schema.of(Schema.Type.BYTES)),
        Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
        Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of("null", Schema.of(Schema.Type.NULL)),
        Schema.Field.of("datetime", Schema.of(Schema.LogicalType.DATETIME))
      ))
      .set("long", expectedLong)
      .set("float", expectedFloat)
      .set("double", expectedDouble)
      .set("string", expectedString)
      .setTimestamp("timestamp_millis", expectedMillis)
      .setTimestamp("timestamp_micros", expectedMicros)
      .setTime("time_millis", expectedTime)
      .setTime("time_micros", expectedTime)
      .set("int", expectedInteger)
      .set("blob", expectedBlob)
      .set("binary", expectedBinary)
      .set("boolean", expectedBoolean)
      .set("null", null)
      .setDateTime("datetime", localDateTime)
      .build();
    List<ColumnType> columnTypes = new ArrayList<ColumnType>();
    columnTypes.add(new ColumnType("long", "long", Types.BIGINT));
    columnTypes.add(new ColumnType("float", "float", Types.FLOAT));
    columnTypes.add(new ColumnType("double", "double", Types.DOUBLE));
    columnTypes.add(new ColumnType("string", "string", Types.VARCHAR));
    columnTypes.add(new ColumnType("timestamp_millis", "timestamp_millis", Types.BIGINT));
    columnTypes.add(new ColumnType("timestamp_micros", "timestamp_micros", Types.BIGINT));
    columnTypes.add(new ColumnType("time_millis", "time_millis", Types.INTEGER));
    columnTypes.add(new ColumnType("time_micros", "time_micros", Types.BIGINT));
    columnTypes.add(new ColumnType("int", "int", Types.INTEGER));
    columnTypes.add(new ColumnType("blob", "blob", Types.BLOB));
    columnTypes.add(new ColumnType("binary", "binary", Types.BINARY));
    columnTypes.add(new ColumnType("boolean", "boolean", Types.INTEGER));
    columnTypes.add(new ColumnType("null", "null", Types.VARCHAR));
    columnTypes.add(new ColumnType("datetime", "datetime", Types.VARCHAR));

    DBRecord record = new DBRecord(input, columnTypes);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    Connection connection = Mockito.mock(Connection.class);
    DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
    Mockito.when(metaData.getDatabaseProductName()).thenReturn("");
    Mockito.when(connection.getMetaData()).thenReturn(metaData);
    Mockito.when(statement.getConnection()).thenReturn(connection);
    record.write(statement);

    Assert.assertEquals(expectedBytesWritten, record.getBytesWritten());
    Mockito.verify(statement, Mockito.times(1)).setLong(1, expectedLong);
    Mockito.verify(statement, Mockito.times(1)).setFloat(2, expectedFloat);
    Mockito.verify(statement, Mockito.times(1)).setDouble(3, expectedDouble);
    Mockito.verify(statement, Mockito.times(1)).setString(4, expectedString);
    Mockito.verify(statement, Mockito.times(1)).setTimestamp(5,
                                                        Timestamp.from(expectedMillis.toInstant()));
    Mockito.verify(statement, Mockito.times(1)).setTimestamp(6,
                                                             Timestamp.from(expectedMicros.toInstant()));
    Mockito.verify(statement, Mockito.times(1)).setTime(7,
                                                        Time.valueOf(expectedTime));
    Mockito.verify(statement, Mockito.times(1)).setTime(8,
                                                        Time.valueOf(expectedTime));
    Mockito.verify(statement, Mockito.times(1)).setInt(9, expectedInteger);
    Mockito.verify(statement, Mockito.times(1)).setBlob(10,
                                                        new SerialBlob(expectedBlob));
    Mockito.verify(statement, Mockito.times(1)).setBytes(11, expectedBinary);
    Mockito.verify(statement, Mockito.times(1)).setBoolean(12, expectedBoolean);
    Mockito.verify(statement, Mockito.times(1)).setNull(13, Types.VARCHAR);
    Mockito.verify(statement, Mockito.times(1)).setString(14, formatedDt);
  }

  @Test
  public void testDBRecordRead() throws Exception {
    ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(rsMetaMock.getColumnCount()).thenReturn(13);
    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(1))).thenReturn("integer");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(1))).thenReturn(ResultSetMetaData.columnNoNulls);
    Mockito.when(rsMetaMock.isSigned(Mockito.eq(1))).thenReturn(true);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(2))).thenReturn("double");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(2))).thenReturn(Types.DOUBLE);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(2))).thenReturn(ResultSetMetaData.columnNullable);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(3))).thenReturn("smallint");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(3))).thenReturn(Types.SMALLINT);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(3))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(4))).thenReturn("tinyint");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(4))).thenReturn(Types.TINYINT);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(4))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(5))).thenReturn("date");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(5))).thenReturn(Types.DATE);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(5))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(6))).thenReturn("time");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(6))).thenReturn(Types.TIME);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(6))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(7))).thenReturn("timestamp");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(7))).thenReturn(Types.TIMESTAMP);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(7))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(8))).thenReturn("decimal");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(8))).thenReturn(Types.DECIMAL);
    Mockito.when(rsMetaMock.getPrecision(Mockito.eq(8))).thenReturn(10);
    Mockito.when(rsMetaMock.getScale(Mockito.eq(8))).thenReturn(3);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(8))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(9))).thenReturn("blob");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(9))).thenReturn(Types.BLOB);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(9))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(10))).thenReturn("boolean");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(10))).thenReturn(Types.BOOLEAN);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(10))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(11))).thenReturn("string");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(11))).thenReturn(Types.VARCHAR);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(11))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(12))).thenReturn("float");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(12))).thenReturn(Types.FLOAT);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(12))).thenReturn(ResultSetMetaData.columnNoNulls);

    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(13))).thenReturn("nullnumeric");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(13))).thenReturn(Types.NUMERIC);
    Mockito.when(rsMetaMock.getPrecision(Mockito.eq(13))).thenReturn(8);
    Mockito.when(rsMetaMock.getScale(Mockito.eq(13))).thenReturn(2);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(13))).thenReturn(ResultSetMetaData.columnNullable);

    Integer expectedInt = 123;
    long intSize = Integer.BYTES;
    Double expectedDouble = 3.14159;
    long doubleSize = Double.BYTES;
    Integer expectedSmallInt = 10;
    long smallIntSize = Integer.BYTES;
    Integer expectedTinyInt = 1;
    long tinyIntSize = Integer.BYTES;
    Date expectedDate = Date.valueOf("2020-02-10");
    long dateSize = Long.BYTES;
    Time expectedTime = Time.valueOf("09:00:00");
    long timeSize = Integer.BYTES;
    Timestamp expectedTimestamp = Timestamp.valueOf("2020-02-10 01:00:00");
    long timestampSize = Long.BYTES;
    BigDecimal expectedDecimal = new BigDecimal("1234567.101");
    long decimalSize = expectedDecimal.unscaledValue().bitLength() / Byte.SIZE + Integer.BYTES;
    byte[] buff = {10, 20, 30, 40};
    Blob expectedBlob = new SerialBlob(buff);
    long blobSize = expectedBlob.length();
    String expectedString = "test";
    long stringSize = expectedString.length();
    float expectedFloat = 3.14f;
    long floatSize = Float.BYTES;
    Boolean expectedBoolean = true;
    long booleanSize = Integer.BYTES;
    BigDecimal expectedNullNumeric = null;
    long nullNumericSize = 0;

    long expectedBytesRead = intSize + doubleSize + smallIntSize + tinyIntSize + dateSize + timeSize + timestampSize +
      decimalSize + blobSize + booleanSize + stringSize + floatSize + nullNumericSize;

    ResultSet resultSetMock = Mockito.mock(ResultSet.class);
    Mockito.when(resultSetMock.getMetaData()).thenReturn(rsMetaMock);
    Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSetMock.getObject("integer")).thenReturn(expectedInt);
    Mockito.when(resultSetMock.getObject("double")).thenReturn(expectedDouble);
    Mockito.when(resultSetMock.getObject("smallint")).thenReturn(expectedSmallInt);
    Mockito.when(resultSetMock.getObject("tinyint")).thenReturn(expectedTinyInt);
    Mockito.when(resultSetMock.getObject("date")).thenReturn(expectedDate);
    Mockito.when(resultSetMock.getDate("date")).thenReturn(expectedDate);
    Mockito.when(resultSetMock.getObject("time")).thenReturn(expectedTime);
    Mockito.when(resultSetMock.getTime("time")).thenReturn(expectedTime);
    Mockito.when(resultSetMock.getObject("timestamp")).thenReturn(expectedTimestamp);
    Mockito.when(resultSetMock.getTimestamp("timestamp")).thenReturn(expectedTimestamp);
    Mockito.when(resultSetMock.getObject("decimal")).thenReturn(expectedDecimal);
    Mockito.when(resultSetMock.getBigDecimal("decimal", 3)).thenReturn(expectedDecimal);
    Mockito.when(resultSetMock.getObject("blob")).thenReturn(expectedBlob);
    Mockito.when(resultSetMock.getObject("boolean")).thenReturn(expectedBoolean);
    Mockito.when(resultSetMock.getObject("string")).thenReturn(expectedString);
    Mockito.when(resultSetMock.getObject("float")).thenReturn(expectedFloat);
    Mockito.when(resultSetMock.getObject("nullnumeric")).thenReturn(expectedNullNumeric);

    StructuredRecord expectedRecord = StructuredRecord
      .builder(Schema.recordOf("dbRecord",
                               Schema.Field.of("integer", Schema.of(Schema.Type.INT)),
                               Schema.Field.of("double", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                               Schema.Field.of("smallint", Schema.of(Schema.Type.INT)),
                               Schema.Field.of("tinyint", Schema.of(Schema.Type.INT)),
                               Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)),
                               Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MICROS)),
                               Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                               Schema.Field.of("decimal", Schema.decimalOf(10, 3)),
                               Schema.Field.of("blob", Schema.of(Schema.Type.BYTES)),
                               Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                               Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                               Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
                               Schema.Field.of("nullnumeric", Schema.nullableOf(
                                 Schema.decimalOf(8, 2)))
                               ))
      .set("integer", expectedInt)
      .set("double", expectedDouble)
      .set("smallint", expectedSmallInt)
      .set("tinyint", expectedTinyInt)
      .setDate("date", expectedDate.toLocalDate())
      .setTime("time", expectedTime.toLocalTime())
      .setTimestamp("timestamp", expectedTimestamp.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
      .setDecimal("decimal", expectedDecimal)
      .set("blob", expectedBlob.getBytes(1, (int) expectedBlob.length()))
      .set("boolean", expectedBoolean)
      .set("string", expectedString)
      .set("float", expectedFloat)
      .set("nullnumeric", expectedNullNumeric)
      .build();

    DBRecord dbRecord = new DBRecord();
    dbRecord.setConf(new Configuration());
    dbRecord.readFields(resultSetMock);
    Assert.assertEquals(expectedBytesRead, dbRecord.getBytesRead());
    Assert.assertEquals(expectedRecord.getSchema(), dbRecord.getRecord().getSchema());
    Assert.assertSame(expectedRecord.get("integer"), dbRecord.getRecord().get("integer"));
    Assert.assertSame(expectedRecord.get("double"), dbRecord.getRecord().get("double"));
    Assert.assertSame(expectedRecord.get("tinyint"), dbRecord.getRecord().get("tinyint"));
    Assert.assertEquals(expectedRecord.getDate("date"), dbRecord.getRecord().getDate("date"));
    Assert.assertEquals(expectedRecord.getTime("time"), dbRecord.getRecord().getTime("time"));
    Assert.assertEquals(expectedRecord.getTimestamp("timestamp"),
                        dbRecord.getRecord().getTimestamp("timestamp"));
    Assert.assertEquals(expectedRecord.getDecimal("decimal"),
                        dbRecord.getRecord().getDecimal("decimal"));
    Assert.assertArrayEquals((byte[]) expectedRecord.get("blob"), (byte[]) dbRecord.getRecord().get("blob"));
    Assert.assertSame(expectedRecord.get("boolean"), dbRecord.getRecord().get("boolean"));
    Assert.assertSame(expectedRecord.get("string"), dbRecord.getRecord().get("string"));
    Assert.assertEquals(0, Float.compare(expectedRecord.get("float"), dbRecord.getRecord().get("float")));
    Assert.assertNull(expectedRecord.getDecimal("nullnumeric"));
  }

  @Test
  public void testDatetime() throws SQLException {
    //When output schema has datetime type , valid datetime string values should be allowed.
    ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(rsMetaMock.getColumnCount()).thenReturn(2);
    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(1))).thenReturn("string");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(1))).thenReturn(Types.VARCHAR);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(1))).thenReturn(ResultSetMetaData.columnNoNulls);
    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(2))).thenReturn("datetimestring");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(2))).thenReturn(ResultSetMetaData.columnNullable);

    String testString = "random";
    LocalDateTime testDateTime = LocalDateTime.now();
    String formattedDateTime = testDateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    ResultSet resultSetMock = Mockito.mock(ResultSet.class);
    Mockito.when(resultSetMock.getMetaData()).thenReturn(rsMetaMock);
    Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSetMock.getObject("string")).thenReturn(testString);
    Mockito.when(resultSetMock.getObject("datetimestring")).thenReturn(formattedDateTime);

    Schema outputSchema = Schema.recordOf("dbRecord",
                                          Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                          Schema.Field
                                            .of("datetimestring",
                                                Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));
    Configuration configuration = new Configuration();
    configuration.set(DBUtils.OVERRIDE_SCHEMA, outputSchema.toString());

    DBRecord dbRecord = new DBRecord();
    dbRecord.setConf(configuration);
    dbRecord.readFields(resultSetMock);

    Assert.assertEquals(testString, dbRecord.getRecord().get("string"));
    Assert.assertEquals(testDateTime, dbRecord.getRecord().getDateTime("datetimestring"));
    Assert.assertEquals(formattedDateTime, dbRecord.getRecord().get("datetimestring"));
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testInvalidDatetime() throws SQLException {
    //When output schema has datetime type , valid datetime string values should be allowed.
    ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(rsMetaMock.getColumnCount()).thenReturn(1);
    Mockito.when(rsMetaMock.getColumnName(Mockito.eq(1))).thenReturn("datetimestring");
    Mockito.when(rsMetaMock.getColumnType(Mockito.eq(1))).thenReturn(Types.VARCHAR);
    Mockito.when(rsMetaMock.isNullable(Mockito.eq(1))).thenReturn(ResultSetMetaData.columnNullable);

    String testDateTime = "Invalid datetime string";
    ResultSet resultSetMock = Mockito.mock(ResultSet.class);
    Mockito.when(resultSetMock.getMetaData()).thenReturn(rsMetaMock);
    Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSetMock.getObject("datetimestring")).thenReturn(testDateTime);

    Schema outputSchema = Schema.recordOf("dbRecord",
                                          Schema.Field.of("datetimestring",
                                                          Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));
    Configuration configuration = new Configuration();
    configuration.set(DBUtils.OVERRIDE_SCHEMA, outputSchema.toString());

    DBRecord dbRecord = new DBRecord();
    dbRecord.setConf(configuration);
    dbRecord.readFields(resultSetMock);
  }
}
