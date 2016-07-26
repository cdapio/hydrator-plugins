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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Custom partitioner class to ensure all data with the a combination of some of the keys specified is sent to the same
 * reducer
 */
public class CompositeKeyPartitioner extends Partitioner<CompositeKey, Text> {
  private final Gson gson = new Gson();
  private final Type schemaType = new TypeToken<Schema>() {  }.getType();

  public int getPartition(CompositeKey comparator, Text value, int numReduceTasks) {
    Schema schema = gson.fromJson(comparator.getSchemaJSON(), schemaType);
    int hash = 7;
    try {
      StructuredRecord structuredRecord1 = StructuredRecordStringConverter
        .fromJsonString(comparator.getStructureRecordJSON(), schema);
      LinkedHashMap<String, String> sortFields = gson.fromJson(comparator.getSortFieldsJSON(), LinkedHashMap.class);
      List<String> sortList = new ArrayList<String>(sortFields.keySet());

      for (int i = 0; i < sortList.size() - 1; i++) {
        String key = sortList.get(i);
        if (key.contains("->")) {
          key = key.split("->")[0].trim();
        }
        hash = 13 * hash + Math.abs((structuredRecord1.get(key) != null ? structuredRecord1.get(key).hashCode() : 0));
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error reading structured record : " + e.getMessage(), e);
    }

    return hash % numReduceTasks;
  }
}
