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
import io.cdap.plugin.format.delimited.input.SplitQuotesIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils facilitating automated schema detection.
 */
public class DataTypeDetectorUtils {

  private static final Pattern NOT_VALID_PATTERN = Pattern.compile("[^A-Za-z0-9_]+");
  private static final Pattern AVRO_NAMING_STANDARD = Pattern.compile("[A-Za-z_]+[A-Za-z0-9_]*");

  /**
   * Detects the data type of each value from a given dataset row;
   *
   * @param override                     Columns with manually specified data types from the user.
   * @param dataTypeDetectorStatusKeeper Object that keeps the state of automated data type detection process.
   * @param columnNames                  Column names.
   * @param rowValues                    Row values.
   */
  public static void detectDataTypeOfRowValues(Map<String, Schema> override,
                                               DataTypeDetectorStatusKeeper dataTypeDetectorStatusKeeper,
                                               String[] columnNames, String[] rowValues) {
    ArrayList<String> rowValuesList = new ArrayList<>(Arrays.asList(rowValues));
    // Pad empty strings at the end if fewer values than required
    // This is the same behaviour exhibited by Spark during pipeline execution
    while (rowValuesList.size() < columnNames.length) {
      rowValuesList.add("");
    }
    for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
      String name = columnNames[columnIndex];
      String value = rowValuesList.get(columnIndex);
      if (!override.containsKey(name)) {
        dataTypeDetectorStatusKeeper.addDataType(name, DataTypeDetectorStatusKeeper.detectValueDataType(value));
      }
    }
  }

  /**
   * Infers column data type for every column in the dataset. If for a column the data type is manually specified,
   * that manually specified data type is taken.
   *
   * @param override                     Columns with manually specified data types from the user.
   * @param dataTypeDetectorStatusKeeper Object that keeps the state of automated data type detection process.
   * @return A list of detected schema fields per each column of the dataset.
   */
  public static List<Schema.Field> detectDataTypeOfEachDatasetColumn(
    Map<String, Schema> override, String[] columnNames, DataTypeDetectorStatusKeeper dataTypeDetectorStatusKeeper) {
    List<Schema.Field> fields = new ArrayList<>();
    for (HashMap.Entry<String, Schema> entry : override.entrySet()) {
      if (!Arrays.asList(columnNames).contains(entry.getKey())) {
        throw new IllegalArgumentException(String.format("Field %s is not present in the input schema!",
                                                         entry.getKey()));
      }
    }

    for (String column : columnNames) {
      if (override.containsKey(column)) {
        fields.add(Schema.Field.of(column, override.get(column)));
      } else {
        Schema inferredSchema = DataTypeDetectorStatusKeeper
          .detectColumnDataType(dataTypeDetectorStatusKeeper.getColumnDataTypes(column));
        fields.add(Schema.Field.of(column, inferredSchema));
      }
    }
    return fields;
  }

  /**
   * A method that sets the column names to the output schema. The `skipHeader=true` means that the first row in
   * the data file is the header row, hence we skip processing it (ie., skip considering it as a data row). Otherwise,
   * if `skipHeader=false` means that the data file does not have the header row (ie., the first row is a data row)
   * so the method automatically generates column names as body_1, body_2,..., body_k.
   *
   * @param rawLine A raw string line read from the dataset.
   * @return Array of column names.
   */
  public static String[] setColumnNames(String rawLine, boolean skipHeader,
                                        boolean enableQuotedValues, String delimiter) {
    String[] columnNames;
    final String quotedDelimiter = Pattern.quote(delimiter);
    if (skipHeader) {
      // need to check enableQuotedValues to remove quotes on headers
      if (enableQuotedValues) {
        // RecordReader and enable multiline support is set to null and false for header values.
        Iterator<String> splitsIterator = new SplitQuotesIterator(rawLine, delimiter, null, false);
        List<String> tempHeaders = new ArrayList<String>();
        while (splitsIterator.hasNext()) {
          tempHeaders.add(splitsIterator.next());
        }
        columnNames = tempHeaders.toArray(new String[tempHeaders.size()]);
      } else {
        // String.split uses regex. Here we safely escape regex sequences by using Pattern.quote
        // Pattern.quote returns a literal pattern string
        columnNames = rawLine.split(quotedDelimiter);
      }
    } else {
      columnNames = new String[rawLine.split(quotedDelimiter, -1).length];
      for (int j = 0; j < columnNames.length; j++) {
        columnNames[j] = String.format("%s_%s", "body", j);
      }
    }
    columnNames = cleanSchemaFieldNames(columnNames);
    validateSchemaFieldNames(columnNames);
    return columnNames;
  }

  /**
   * Validates whether the given column name complies with avro field naming standard. Field names can start with
   * [A-Za-z_] and subsequently contain only [A-Za-z0-9_].
   *
   * @param fieldNames an array of field names to be validated
   */
  public static void validateSchemaFieldNames(String[] fieldNames) {
    for (String fieldName : fieldNames) {
      Matcher matcher = AVRO_NAMING_STANDARD.matcher(fieldName);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
          String.format(
            "Invalid column name detected: \"%s\"! " +
              "Column names can only start with capital letters, small letters, and " + "\"_\". " +
              "Subsequently they may contain only capital letters, small letters, numbers  and \"_\".", fieldName
          )
        );
      }
    }
  }

  /**
   * Cleans an array of field names to make sure they comply with avro field naming standard.
   * It also makes sure each name is unique in the list.
   * Field names can start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_].
   * <p>
   * Steps:
   * 1) Trim surrounding spaces
   * 2) If its empty replace it with BLANK
   * 3) If it starts with a number, prepend "col_"
   * 4) Replace invalid characters with "_" (multiple invalid characters gets replaced with one symbol)
   * 5) Check if the name has been found before (without considering case)
   * if so add _# where # is the number of times seen before + 1
   *
   * @param fieldNames an array of field names to be cleaned
   */
  private static String[] cleanSchemaFieldNames(String[] fieldNames) {
    final String replacementChar = "_";
    final List<String> cleanFieldNames = new ArrayList<>();
    final Map<String, Integer> seenFieldNames = new HashMap<>();
    for (String fieldName : fieldNames) {
      StringBuilder cleanFieldNameBuilder = new StringBuilder();

      // Remove any spaces at the end of the strings
      fieldName = fieldName.trim();

      // If it's an empty string replace it with BLANK
      if (fieldName.length() == 0) {
        cleanFieldNameBuilder.append("BLANK");
      } else if ((fieldName.charAt(0) >= '0') && (fieldName.charAt(0) <= '9')) {
        // Prepend a col_ if the first character is a number
        cleanFieldNameBuilder.append("col_");
      }

      // Replace all invalid characters with the replacement char
      cleanFieldNameBuilder.append(NOT_VALID_PATTERN.matcher(fieldName).replaceAll(replacementChar));

      // Check if the field exist if so append and index at the end
      // We use lowercase to match columns "A" and "a" to avoid issues with wrangler.
      String cleanFieldName = cleanFieldNameBuilder.toString();
      String lowerCaseCleanFieldName = cleanFieldName.toLowerCase();
      while (seenFieldNames.containsKey(lowerCaseCleanFieldName)) {
        cleanFieldNameBuilder.append(replacementChar).append(seenFieldNames.get(lowerCaseCleanFieldName));
        seenFieldNames.put(lowerCaseCleanFieldName, seenFieldNames.get(lowerCaseCleanFieldName) + 1);
        cleanFieldName = cleanFieldNameBuilder.toString();
        lowerCaseCleanFieldName = cleanFieldName.toLowerCase();
      }
      seenFieldNames.put(lowerCaseCleanFieldName, 2);

      cleanFieldNames.add(cleanFieldName);
    }
    return cleanFieldNames.toArray(new String[0]);
  }
}
