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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Maps a record to a key field plus the record.
 */
public class RecordToKeyRecordPairFunction implements PairFunction<StructuredRecord, SCD2Key, StructuredRecord> {
  private final String keyField;
  private final String startDateField;

  public RecordToKeyRecordPairFunction(String keyField, String startDateField) {
    this.keyField = keyField;
    this.startDateField = startDateField;
  }

  @Override
  public Tuple2<SCD2Key, StructuredRecord> call(StructuredRecord record) {
    // TODO: how should null keys be handled? or null start times?
    Object key = record.get(keyField);
    if (key == null) {
      throw new IllegalArgumentException("The key should not be null");
    }
    return new Tuple2<>(new SCD2Key((Comparable) key, record.get(startDateField)), record);
  }
}

