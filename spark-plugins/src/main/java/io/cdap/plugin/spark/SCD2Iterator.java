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

package io.cdap.plugin.spark;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The scd2 iterator, it keeps track of cur, prev, next from the given iterator.
 */
public class SCD2Iterator extends AbstractIterator<StructuredRecord> {
  // 9999-12-31 00:00:00 timestamp in micro seconds
  private static final long ACTIVE_TS = 253402214400000000L;
  private final Iterator<Tuple2<SCD2Key, StructuredRecord>> records;
  private final Table<Object, String, Object> valTable;
  private final SCD2.Conf conf;
  private final Set<String> blacklist;
  private final Set<String> placeholders;
  private Schema outputSchema;
  private Tuple2<SCD2Key, StructuredRecord> cur;
  private Tuple2<SCD2Key, StructuredRecord> prev;
  private Tuple2<SCD2Key, StructuredRecord> next;

  public SCD2Iterator(Iterator<Tuple2<SCD2Key, StructuredRecord>> records, SCD2.Conf conf) {
    this.records = records;
    this.conf = conf;
    this.blacklist = conf.getBlacklist();
    this.valTable = HashBasedTable.create();
    this.placeholders = conf.getPlaceHolderFields();
  }

  @Override
  protected StructuredRecord computeNext() {
    // if the records does not have value, but next still have a value, we still need to process it
    if (!records.hasNext() && next == null) {
      return endOfData();
    }

    prev = cur;
    cur = next != null ? next : records.next();
    next = records.hasNext() ? records.next() : null;

    // deduplicate the result
    if (conf.deduplicate() && next != null && next._1().equals(cur._1())) {
      boolean isDiff = false;
      for (Schema.Field field : cur._2().getSchema().getFields()) {
        String fieldName = field.getName();
        Object value = cur._2().get(fieldName);
        if (blacklist.contains(fieldName)) {
          continue;
        }

        // check if there is difference between next record and cur record
        Object nextVal = next._2().get(fieldName);
        if ((nextVal == null) != (value == null) || (value != null && !value.equals(nextVal))) {
          isDiff = true;
          break;
        }
      }
      if (!isDiff) {
        return null;
      }
    }

    // if key changes, clean up the table to free memory
    if (prev != null && !prev._1().equals(cur._1())) {
      valTable.row(prev._1().getKey()).clear();
    }

    return computeRecord(cur._1().getKey(),
                         prev != null && prev._1().equals(cur._1()) ? prev._2() : null,
                         cur._2(),
                         next != null && next._1().equals(cur._1()) ? next._2() : null);
  }

  private StructuredRecord computeRecord(Object key, @Nullable StructuredRecord prev, StructuredRecord cur,
                                         @Nullable StructuredRecord next) {
    if (outputSchema == null) {
      outputSchema = conf.getOutputSchema(cur.getSchema());
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    for (Schema.Field field : cur.getSchema().getFields()) {
      String fieldName = field.getName();
      Object value = cur.get(fieldName);
      // if enable scd2 hybrid, check for prev record is a late arriving data which does not have an end date,
      // also make sure the current record is either closed or is not late arriving date which has a end date
      if (conf.enableHybridSCD2() && placeholders.contains(fieldName) && prev != null &&
            prev.get(conf.getEndDateField()) == null && (next == null || cur.get(conf.getEndDateField()) != null)) {
        value = prev.get(fieldName);
      }

      // fill in null from previous record
      if (conf.fillInNull() && value == null) {
        value = valTable.get(key, fieldName);
      }
      builder.set(fieldName, value);
      if (conf.fillInNull() && value != null) {
        valTable.put(key, fieldName, value);
      }
    }

    long endDate;
    if (next == null) {
      endDate = ACTIVE_TS;
    } else {
      Long date = next.get(conf.getStartDateField());
      // TODO: handle nulls in start date? Or simply restrict the schema to be non-nullable
      endDate = date == null ? ACTIVE_TS : date - 1000000L;
    }
    builder.set(conf.getEndDateField(), endDate);
    return builder.build();
  }
}
