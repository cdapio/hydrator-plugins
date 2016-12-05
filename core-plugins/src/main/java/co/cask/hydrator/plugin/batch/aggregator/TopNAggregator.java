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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchAggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("TopNAggregator")
@Description("Top N aggregator")
public class TopNAggregator extends BatchAggregator<StructuredRecord, StructuredRecord, StructuredRecord> {
  private final TopNAggregatorConfig conf;
  private final Map<String, Long> countMap = new HashMap<>();

  public TopNAggregator(TopNAggregatorConfig conf) {
    this.conf = conf;
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
    String groupByField = conf.groupByField;
    builder.set(groupByField, record.get(groupByField));
    emitter.emit(builder.build());
  }

  @Override
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (!iterator.hasNext()) {
      return;
    }

    String s = conf.topN;
    int topN = Integer.parseInt(s);

    for (int i = 0; i < topN; i++) {

      if (!iterator.hasNext()) {
        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
          List<Schema.Field> outputFields = new ArrayList<>();
          outputFields.add(Schema.Field.of("word", Schema.of(Schema.Type.STRING)));
          outputFields.add(Schema.Field.of("wordCount", Schema.of(Schema.Type.LONG)));
          Schema outputSchema = Schema.recordOf("word.count", outputFields);

          StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema)
            .set("word", entry.getKey())
            .set("wordCount", entry.getValue());
          emitter.emit(builder.build());
        }
      }

      StructuredRecord value = iterator.next();
      String word = value.get("word");
      Long wordCount = value.get("wordCount");
      countMap.put(word, wordCount);
    }

    while (iterator.hasNext()) {
      StructuredRecord value = iterator.next();
      String word = value.get("word");
      Long wordCount = value.get("wordCount");
      List<Map.Entry<String, Long>> entries = sortCountMap();


      Map.Entry<String, Long> entry = entries.get(0);

      if (entry.getValue() < wordCount) {
        entries.remove(0);
        countMap.remove(entry.getKey());
        countMap.put(word, wordCount);
      }
    }

    // sort count map
    List<Map.Entry<String, Long>> entries = new ArrayList<>(countMap.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
      @Override
      public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });

    // Generate output Schema
    List<Schema.Field> outputFields = new ArrayList<>();
    outputFields.add(Schema.Field.of("word", Schema.of(Schema.Type.STRING)));
    outputFields.add(Schema.Field.of("wordCount", Schema.of(Schema.Type.LONG)));

    Schema outputSchema = Schema.recordOf("topN", outputFields);

    for (int i = 0; i < topN; i++) {
      Map.Entry<String, Long> entry = entries.get(i);
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema)
        .set("word", entry.getKey())
        .set("wordCount", entry.getValue());
      // emit record
      emitter.emit(builder.build());
    }

  }

  private List<Map.Entry<String, Long>> sortCountMap() {
    // sort the hashmap
    List<Map.Entry<String, Long>> entries = new ArrayList<>(countMap.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
      @Override
      public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });
    return entries;
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    Schema.Field fieldSchema = inputSchema.getField(conf.groupByField);
    fields.add(fieldSchema);
    return Schema.recordOf("group.key.schema", fields);
  }

  /**
   * Config class for ProjectionTransform
   */
  public static class TopNAggregatorConfig extends PluginConfig {


    @Name("groupByField")
    @Description("Groupby field")
    private final String groupByField;

    @Name("topN")
    @Description("value of n")
    String topN;

    public TopNAggregatorConfig(String groupByField, String topN) {
      this.groupByField = groupByField;
      this.topN = topN;
    }
  }
}
