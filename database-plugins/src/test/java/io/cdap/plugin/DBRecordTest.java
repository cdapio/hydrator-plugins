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
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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


    long expectedBytesWritten = longSize + floatSize + doubleSize + stringSize + timeSize * 2 + integerSize +
      timeMillisSize + timeMicrosSize + blobSize + binarySize + booleanSize;
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
        Schema.Field.of("null", Schema.of(Schema.Type.NULL))
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
      .build();
    int[] columnTypes = {Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.VARCHAR, Types.BIGINT, Types.BIGINT,
      Types.INTEGER, Types.BIGINT, Types.INTEGER, Types.BLOB, Types.BINARY, Types.INTEGER, Types.VARCHAR};
    DBRecord record = new DBRecord(input, columnTypes);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
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
  }
}
