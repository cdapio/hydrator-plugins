/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.dq;

import io.cdap.plugin.format.delimited.common.DataType;
import io.cdap.plugin.format.delimited.common.TypeInference;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link TypeInference}
 */
public class TypeInferenceTest {

  @Test
  public void testIsDouble() {
    assertTrue(TypeInference.isDouble("-12.3"));
  }

  @Test
  public void testIsNotDouble() {
    assertFalse(TypeInference.isDouble("Hello!"));
  }

  @Test
  public void testIsLong() {
    assertTrue(TypeInference.isLong("123L"));
  }

  @Test
  public void testIsNotLong() {
    assertFalse(TypeInference.isLong("12.3"));
  }

  @Test
  public void testIsInteger() {
    assertTrue(TypeInference.isInteger("123"));
  }

  @Test
  public void testIsNotInteger() {
    assertFalse(TypeInference.isInteger("1.23"));
  }

  @Test
  public void testIsBoolean() {
    assertTrue(TypeInference.isBoolean("false"));
  }

  @Test
  public void testIsNotBoolean() {
    assertFalse(TypeInference.isBoolean("Hello, World!"));
  }

  @Test
  public void testIsDate() {
    assertTrue(TypeInference.isDate("2021-01-12T23:50:06+00:00"));
  }

  @Test
  public void testIsNotDate() {
    assertFalse(TypeInference.isDate("Wednesday"));
  }

  @Test
  public void testIsTime() {
    assertTrue(TypeInference.isTime("23:50:06"));
  }

  @Test
  public void testIsNotTime() {
    assertFalse(TypeInference.isTime("2021-01-12"));
  }

  @Test
  public void testIsEmpty() {
    assertTrue(TypeInference.isEmpty(""));
  }

  @Test
  public void testIsNotEmpty() {
    assertFalse(TypeInference.isTime("123"));
  }

  @Test
  public void testGetDataTypeInt() {
    // Every integer (math) number, in the range [-2147483648, 2147483647] produces an int data type
    String firstValue = String.valueOf(Integer.MAX_VALUE);
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.INTEGER);

    String secondValue = String.valueOf(Integer.MIN_VALUE);
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.INTEGER);
  }

  @Test
  public void testGetDataTypeLong() {
    // Every integer (math) number, in the range [-9223372036854775808, 9223372036854775807] but not in [-2147483648,
    // 2147483647] produces a long data type
    String firstValue = String.valueOf(Integer.MAX_VALUE) + "0";
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.LONG);

    String secondValue = String.valueOf(Integer.MIN_VALUE) + "0";
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.LONG);

    // Every integer (math) number, in the range [-9223372036854775808, 9223372036854775807] with an L suffix is
    // produces a long data type
    String thirdValue = "-1000L";
    DataType thirdDataType = TypeInference.getDataType(thirdValue);
    assertEquals(secondDataType, DataType.LONG);

    // Every integer (math) number, out of the range [-9223372036854775808, 9223372036854775807] produces a string
    // data type
    String fourthValue = String.valueOf(Long.MAX_VALUE) + "0";
    DataType fourthDataType = TypeInference.getDataType(fourthValue);
    assertNotEquals(fourthDataType, DataType.LONG);
    assertEquals(fourthDataType, DataType.STRING);
  }

  @Test
  public void testGetDataTypeDouble() {
    String firstValue = "3.14";
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.DOUBLE);

    String secondValue = "-3.14";
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.DOUBLE);

    // Every value that when parsed to Double gets the number of digits shorten, we consider it as a Big Decimal
    // number. Hence we keep it as a string.
    String thirdValue = "13004.012312312423112122121121212";
    DataType thirdDataType = TypeInference.getDataType(thirdValue);
    assertEquals(thirdDataType, DataType.STRING);
  }


  @Test
  public void testGetDataTypeBoolean() {
    String firstValue = "true";
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.BOOLEAN);

    String secondValue = "false";
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.BOOLEAN);

    String thirdValue = "FaLsE";
    DataType thirdDataType = TypeInference.getDataType(thirdValue);
    assertEquals(thirdDataType, DataType.BOOLEAN);

    String fourthValue = "yes";
    DataType fourthDataType = TypeInference.getDataType(fourthValue);
    assertNotEquals(fourthDataType, DataType.BOOLEAN);
  }

  @Test
  public void testGetDataTypeString() {
    String firstValue = "Hello, World!";
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.STRING);

    String secondValue = Long.MAX_VALUE + "0";
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.STRING);
  }

  @Test
  public void testGetDataTypeEmpty() {
    String firstValue = "";
    DataType firstDataType = TypeInference.getDataType(firstValue);
    assertEquals(firstDataType, DataType.EMPTY);

    String secondValue = "     ";
    DataType secondDataType = TypeInference.getDataType(secondValue);
    assertEquals(secondDataType, DataType.EMPTY);

    String thirdValue = null;
    DataType thirdDataType = TypeInference.getDataType(thirdValue);
    assertEquals(thirdDataType, DataType.EMPTY);
  }

  @Test
  public void testGetDataTypeDate() {
    List<String> dateStringValues = Arrays.asList(
            "2021-01-01", "2009-05-19T14:39Z", "2009-W33", "2021-02-18T16:23:48,3-06:00", "2021-04-05T24:00",
            "2021-02-18T16.23334444", "2021-03-04T23:05:11+00:00", "2021-03-04T23:05:11Z", "20210304T230511Z"
    );
    dateStringValues.forEach(val -> assertEquals(TypeInference.getDataType(val), DataType.DATE));
  }

  @Test
  public void testGetDataTypeTime() {
    List<String> timeStringValues = Arrays.asList("12:00:00", "23:03", "23:59:59Z");
    timeStringValues.forEach(val -> assertEquals(TypeInference.getDataType(val), DataType.TIME));

  }
}
