/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * CustomWritable for the composite key.
 */
public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

  public static final String FIELD_DELIMITER = ":";
  //Since 0, 1 and -1 are used for record comparison; using 100 to identify that the records do not contain null.
  private static final int DEFAULT_COMP_VALUE = 100;
  private final Gson gson = new GsonBuilder().create();
  private final Type mapType = new TypeToken<LinkedHashMap>() {  }.getType();
  private final Type schemaType = new TypeToken<Schema>() {  }.getType();
  private String structuredRecordJSON;
  private String sortFieldsJSON;
  private String schemaJSON;

  public CompositeKey() {
  }

  public CompositeKey(Text structuredRecordJSON, String sortFieldsJSON, String schemaJSON) {
    this.structuredRecordJSON = structuredRecordJSON.toString();
    this.sortFieldsJSON = sortFieldsJSON;
    this.schemaJSON = schemaJSON;
  }

  public static int getSortingOrder(String order) {
    if (order.equalsIgnoreCase("asc")) {
      return 1;
    } else {
      return -1;
    }
  }

  /**
   * Get sorting order for structured records depending on the type of value contained in key
   *
   * @param key               contains the value to sort the data
   * @param value             determines whether the objects are to be sorted in ascending or descending order for key
   * @param structuredRecord1 Structured record
   * @param structuredRecord2 Structured record to compare against
   * @return sorting order of the complete structured record
   */
  public static int compareRecords(String key, String value, StructuredRecord structuredRecord1,
                                   StructuredRecord structuredRecord2) {
    int res;
    res = compareNull(key, structuredRecord1, structuredRecord2);
    if (res == DEFAULT_COMP_VALUE) {
      Schema schema = structuredRecord1.getSchema().getField(key).getSchema();
      Schema.Type type = schema.isNullableSimple() ? schema.getNonNullable().getType() : schema.getType();
      if (type.equals(Schema.Type.STRING)) {
        res = getSortingOrder(value) * ((String) structuredRecord1.get(key))
          .compareTo((String) structuredRecord2.get(key));
      } else if (type.equals(Schema.Type.INT)) {
        res = getSortingOrder(value) * Integer.compare((Integer) structuredRecord1.get(key),
                                                       (Integer) structuredRecord2.get(key));
      } else if (type.equals(Schema.Type.LONG)) {
        res = getSortingOrder(value) * Long.compare((Long) structuredRecord1.get(key),
                                                    (Long) structuredRecord2.get(key));
      } else if (type.equals(Schema.Type.FLOAT)) {
        res = getSortingOrder(value) * Float.compare((Float) structuredRecord1.get(key),
                                                     (Float) structuredRecord2.get(key));
      } else if (type.equals(Schema.Type.DOUBLE)) {
        res = getSortingOrder(value) * Double.compare((Double) structuredRecord1.get(key),
                                                      (Double) structuredRecord2.get(key));
      } else if (type.equals(Schema.Type.BYTES)) {
        res = getSortingOrder(value) * Byte.compare((Byte) structuredRecord1.get(key),
                                                    (Byte) structuredRecord2.get(key));
      } else {
        throw new IllegalArgumentException("Sorting can be performed only on the following CDAP data types:  " +
                                             "String, Int, Long, Float, Double, Byte");
      }
    }
    return res;
  }

  /**
   * Compares key fields with null values. Also, places records with null values in the key field at the end of the
   * list.
   *
   * @param key               contains the value to sort the data
   * @param structuredRecord1 Structured record
   * @param structuredRecord2 Structured record to compare against
   * @return sorting order of the complete structured record
   */
  private static int compareNull(String key, StructuredRecord structuredRecord1, StructuredRecord structuredRecord2) {
    int result = DEFAULT_COMP_VALUE;
    Schema fieldSchema = structuredRecord1.getSchema().getField(key).getSchema();
    if (!fieldSchema.isNullableSimple()) {
      if (Strings.isNullOrEmpty(structuredRecord1.get(key).toString()) ||
        Strings.isNullOrEmpty(structuredRecord2.get(key).toString())) {
        throw new IllegalArgumentException(String.format("Cannot accept null values for non-nullable field %s ", key));
      }
    }
    if (structuredRecord1.get(key) == null && structuredRecord2.get(key) == null) {
      result = 0;
    } else if (structuredRecord1.get(key) == null) {
      result = 1;
    } else if (structuredRecord2.get(key) == null) {
      result = -1;
    }
    return result;
  }

  @Override
  /**
   *  Write method for serialization. Should be in sync with read()
   */
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(structuredRecordJSON);
    dataOutput.writeUTF(sortFieldsJSON);
    dataOutput.writeUTF(schemaJSON);
  }

  @Override
  /**
   * Read method for serialization.
   */
  public void readFields(DataInput dataInput) throws IOException {
    this.structuredRecordJSON = dataInput.readUTF();
    this.sortFieldsJSON = dataInput.readUTF();
    this.schemaJSON = dataInput.readUTF();
  }

  /**
   * This comparator controls the sort order of the keys.
   *
   * @param compositeKeyToCompare Compositekey for comparison
   * @return structured records sorted on the keys
   */
  @Override
  public int compareTo(CompositeKey compositeKeyToCompare) {
    int res = 0;
    Schema schema = gson.fromJson(schemaJSON, schemaType);
    try {
      StructuredRecord structuredRecord1 = StructuredRecordStringConverter
        .fromJsonString(this.structuredRecordJSON, schema);
      StructuredRecord structuredRecord2 = StructuredRecordStringConverter
        .fromJsonString(compositeKeyToCompare.structuredRecordJSON, schema);
      LinkedHashMap<String, String> sortFields = gson.fromJson(sortFieldsJSON, mapType);

      for (Map.Entry<String, String> entry : sortFields.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key.contains(FIELD_DELIMITER)) {
          res = compareNestedRecords(key, value, structuredRecord1, structuredRecord2);
        } else {
          res = compareRecords(key, value, structuredRecord1, structuredRecord2);
        }
        if (res != 0) {
          return res;
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error reading structured record : " + e.getMessage(), e);
    }
    return res;
  }

  /**
   * Get sorting order for nested structured records using recursive call
   *
   * @param key               contains the value to sort the data
   * @param value             determines whether the objects are to be sorted in ascending or descending order for key
   * @param structuredRecord1 first Structured record
   * @param structuredRecord2 second Structured record to compare against
   * @return structured records sorted on the keys
   */
  private int compareNestedRecords(String key, String value, StructuredRecord structuredRecord1,
                                   StructuredRecord structuredRecord2) {

    if (key.contains(FIELD_DELIMITER)) {
      String[] nestedRecord = key.split(FIELD_DELIMITER);
      Schema.Field field = structuredRecord1.getSchema().getField(nestedRecord[0].trim());
      Schema schema = field.getSchema();
      Schema.Type type = schema.isNullableSimple() ? schema.getNonNullable().getType() : schema.getType();
      if (type.equals(Schema.Type.RECORD)) {
        structuredRecord1 = structuredRecord1.get(nestedRecord[0].trim());
        structuredRecord2 = structuredRecord2.get(nestedRecord[0].trim());
        nestedRecord = (String[]) ArrayUtils.removeElement(nestedRecord, nestedRecord[0].trim());
        key = Joiner.on(FIELD_DELIMITER).join(nestedRecord);
        int res = compareNull(key, structuredRecord1, structuredRecord2);
        if (res != DEFAULT_COMP_VALUE) {
          return res;
        } else {
          return compareNestedRecords(key, value, structuredRecord1, structuredRecord2);
        }
      } else {
        throw new IllegalArgumentException("Nested structured defined by \":\" should be of type RECORD");
      }
    } else {
      return compareRecords(key, value, structuredRecord1, structuredRecord2);
    }
  }

  public String getStructuredRecordJSON() {
    return structuredRecordJSON;
  }

  public String getSortFieldsJSON() {
    return sortFieldsJSON;
  }

  public String getSchemaJSON() {
    return schemaJSON;
  }
}
