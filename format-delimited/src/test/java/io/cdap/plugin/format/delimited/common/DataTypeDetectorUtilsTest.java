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

package io.cdap.plugin.format.delimited.common;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DataTypeDetectorUtils}
 */
public class DataTypeDetectorUtilsTest {

  @Test
  public void testExtractedColumnNames() {
    String headerLine = "column_A;column_B;column_C";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(headerLine, true, false, ";", null);
    String[] expectedColumnNames = new String[]{"column_A", "column_B", "column_C"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testExtractedColumnNamesWithPathField() {
    String headerLine = "column_A|column_B|column_C";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(headerLine, true, false, "|", "pathField");
    String[] expectedColumnNames = new String[]{"column_A", "column_B", "column_C", "pathField"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testExtractedColumnNamesRegexMetaCharacterDelimiter() {
    String headerLine = "column_A|column_B|column_C";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(headerLine, true, false, "|", null);
    String[] expectedColumnNames = new String[]{"column_A", "column_B", "column_C"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testExtractedQuotedColumnNames() {
    String headerLine = "\"column_A\";\"column_B\";\"column_C\"";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(headerLine, true, true, ";", null);
    String[] expectedColumnNames = new String[]{"column_A", "column_B", "column_C"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testExtractedQuotedColumnNamesRegexMetaCharacterDelimiter() {
    String headerLine = "\"column_A\"|\"column_B\"|\"column_C\"";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(headerLine, true, true, "|", null);
    String[] expectedColumnNames = new String[]{"column_A", "column_B", "column_C"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testGeneratedColumnNamesForRegexMetaCharacterDelimiter() {
    String dataLine = "John|Doe|27";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(dataLine, false, false, "|", null);
    String[] expectedColumnNames = new String[]{"body_0", "body_1", "body_2"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testGeneratedColumnNamesWithPathField() {
      String dataLine = "Col1|Col2|Col3";
      String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(dataLine, false, false, "|", "pathField");
      String[] expectedColumnNames = new String[]{"body_0", "body_1", "body_2", "pathField"};
      assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testGeneratedColumnNames() {
    String dataLine = "John;Doe;27";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(dataLine, false, false, ";", null);
    String[] expectedColumnNames = new String[]{"body_0", "body_1", "body_2"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testNonAvroStandardFieldNameShouldBeReplaced() {
    String fieldNames = "\"column-1\", \"1column\", \"1234\", \" column#a\", \"\", \",\", \" \", \"_\", " +
      "\"column_1\", \"column_1\", \"column_1_2\", \" s p a c e s \", \"1!)@#*$%&!@\"";
    String[] actualColumnNames = DataTypeDetectorUtils.setColumnNames(
      fieldNames, true, true, ",", null);
    String[] expectedColumnNames = new String[]{"column_1", "col_1column", "col_1234", "column_a", "BLANK",
      "_", "BLANK_2", "__2", "column_1_2", "column_1_3", "column_1_2_2", "s_p_a_c_e_s", "col_1_"};
    assertArrayEquals(expectedColumnNames, actualColumnNames);
  }

  @Test
  public void testNonStandardFieldNamesShouldValidateSuccessfully() {
    String[] fieldNames = new String[]{"column", "column_1", "_column", "Column", "_COLUMN_1_2_"};
    DataTypeDetectorUtils.validateSchemaFieldNames(fieldNames);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonAvroStandardFieldNamesShouldThrowError() {
    String[] fieldNames = new String[]{"column-1", "1column", "1234", "column#a"};
    DataTypeDetectorUtils.validateSchemaFieldNames(fieldNames);
  }

  @Test
  public void testDetectDataTypeOfEachDatasetColumn() {
    DataTypeDetectorStatusKeeper dataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    String[] columnNames = new String[]{"column_A", "column_B", "column_C", "column_D", "column_E", "column_F"};
    Map<String, EnumSet<DataType>> status = new HashMap<>();
    status.put(columnNames[0], EnumSet.of(DataType.INTEGER));
    status.put(columnNames[1], EnumSet.of(DataType.INTEGER, DataType.LONG));
    status.put(columnNames[2], EnumSet.of(DataType.INTEGER, DataType.DOUBLE));
    status.put(columnNames[3], EnumSet.of(DataType.BOOLEAN));
    status.put(columnNames[4], EnumSet.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE));
    status.put(columnNames[5], EnumSet.of(DataType.DOUBLE, DataType.STRING));
    dataTypeDetectorStatusKeeper.setDataTypeDetectionStatus(status);

    Map<String, Schema> override = new HashMap<>();
    override.put("column_A", Schema.of(Schema.Type.DOUBLE));

    List<Schema> actualFields = DataTypeDetectorUtils
      .detectDataTypeOfEachDatasetColumn(override, columnNames, dataTypeDetectorStatusKeeper)
      .stream().map(Schema.Field::getSchema).collect(Collectors.toList());

    List<Schema> expectedFields = new ArrayList<>(Arrays.asList(
      Schema.Field.of(columnNames[0], Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(columnNames[1], Schema.of(Schema.Type.LONG)),
      Schema.Field.of(columnNames[2], Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(columnNames[3], Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of(columnNames[4], Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(columnNames[5], Schema.of(Schema.Type.STRING))
    )).stream().map(Schema.Field::getSchema).collect(Collectors.toList());

    assertEquals(expectedFields, actualFields);
  }

  @Test
  public void testDetectDataTypeOfRowValue() {
    DataTypeDetectorStatusKeeper actualDataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    String[] columnNames = new String[]{"column_A", "column_B", "column_C", "column_D", "column_E", "column_F"};
    String[] rowValues = new String[]{"2", "24L", "3.14", "false", "100.05", "100A"};

    DataTypeDetectorUtils.detectDataTypeOfRowValues(new HashMap<>(), actualDataTypeDetectorStatusKeeper, columnNames,
                                                    rowValues);

    DataTypeDetectorStatusKeeper expectedDataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    Map<String, EnumSet<DataType>> status = new HashMap<>();
    status.put(columnNames[0], EnumSet.of(DataType.INTEGER));
    status.put(columnNames[1], EnumSet.of(DataType.LONG));
    status.put(columnNames[2], EnumSet.of(DataType.DOUBLE));
    status.put(columnNames[3], EnumSet.of(DataType.BOOLEAN));
    status.put(columnNames[4], EnumSet.of(DataType.DOUBLE));
    status.put(columnNames[5], EnumSet.of(DataType.STRING));
    expectedDataTypeDetectorStatusKeeper.setDataTypeDetectionStatus(status);

    assertEquals(expectedDataTypeDetectorStatusKeeper, actualDataTypeDetectorStatusKeeper);
  }

  @Test
  public void testDetectDataTypeOfEachDatasetColumnWithMissingValues() {
    String[] columnNames = new String[]{"column_A", "column_B", "column_C"};
    String[] rowValues1 = new String[]{"100A", "2020", "129.99"};
    // Row 2 has two values instead of 3, so empty string will be padded at the end
    String[] rowValues2 = new String[]{"Happy", "17.99"};
    DataTypeDetectorStatusKeeper actualDataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    DataTypeDetectorUtils.detectDataTypeOfRowValues(new HashMap<>(), actualDataTypeDetectorStatusKeeper, columnNames,
                                                    rowValues1);
    DataTypeDetectorUtils.detectDataTypeOfRowValues(new HashMap<>(), actualDataTypeDetectorStatusKeeper, columnNames,
                                                    rowValues2);
    actualDataTypeDetectorStatusKeeper.validateDataTypeDetector();
    Map<String, Schema> override = new HashMap<>();

    List<Schema> actualFields = DataTypeDetectorUtils
      .detectDataTypeOfEachDatasetColumn(override, columnNames, actualDataTypeDetectorStatusKeeper)
      .stream().map(Schema.Field::getSchema).collect(Collectors.toList());

    List<Schema> expectedFields = new ArrayList<>(Arrays.asList(
      Schema.Field.of(columnNames[0], Schema.of(Schema.Type.STRING)),
      Schema.Field.of(columnNames[1], Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(columnNames[2], Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    )).stream().map(Schema.Field::getSchema).collect(Collectors.toList());

    assertEquals(expectedFields, actualFields);
  }
}
