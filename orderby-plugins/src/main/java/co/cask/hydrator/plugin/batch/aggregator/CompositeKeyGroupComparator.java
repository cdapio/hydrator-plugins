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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Custom group comparator to control which keys are grouped together into a single call to reduce()
 */
public class CompositeKeyGroupComparator extends WritableComparator {
  private final Gson gson = new Gson();

  public CompositeKeyGroupComparator() {
    super(CompositeKey.class, true);
  }

  @Override
  /**
   * This comparator controls which keys are grouped together into a single call to the reduce() method
   * @param wc1 CompositeKey object for sorting
   * @param wc2 CompositeKey object for sorting
   * @return structured records grouped and sorted based on some of the keys
   */
  public int compare(WritableComparable wc1, WritableComparable wc2) {
    CompositeKey comparator1 = (CompositeKey) wc1;
    CompositeKey comparator2 = (CompositeKey) wc2;
    int res = 0;
    Type schemaType = new TypeToken<Schema>() {    }.getType();
    Schema schema = gson.fromJson(comparator1.getSchemaJSON(), schemaType);
    try {
      StructuredRecord structuredRecord1 = StructuredRecordStringConverter
        .fromJsonString(comparator1.getStructuredRecordJSON(), schema);
      StructuredRecord structuredRecord2 = StructuredRecordStringConverter
        .fromJsonString(comparator2.getStructuredRecordJSON(), schema);
      LinkedHashMap<String, String> sortFields = gson.fromJson(comparator1.getSortFieldsJSON(), LinkedHashMap.class);
      List<String> sortList = new ArrayList<String>(sortFields.keySet());
      for (int i = 0; i < sortList.size() - 1; i++) {
        String key = sortList.get(i);
        String value = sortFields.get(key);
        if (key.contains(CompositeKey.FIELD_DELIMITER)) {
          key = key.split(CompositeKey.FIELD_DELIMITER)[0].trim();
        }
        if (structuredRecord1.getSchema().getField(key).getSchema().getType().equals(Schema.Type.RECORD)) {
          res = CompositeKey.getSortingOrder(value) * Integer.compare(structuredRecord1.get(key).hashCode(),
                                                                      structuredRecord2.get(key).hashCode());
        } else {
          res = CompositeKey.compareRecords(key, value, structuredRecord1, structuredRecord2);
        }

        if (res == 0) {
          return res;
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error reading structured record : " + e.getMessage(), e);
    }

    return res;
  }
}
