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

package co.cask.hydrator.plugin.batch.aggregator.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link StructuredRecord} that can be used to select the record with the min value of a given field.
 */
public class MinSelection extends NumberSelection {
  private StructuredRecord minRecord;
  private Integer minInt;
  private Long minLong;
  private Float minFloat;
  private Double minDouble;

  public MinSelection(String fieldName, Schema fieldSchema) {
    super(fieldName, fieldSchema);
    minRecord = null;
  }

  @Override
  protected void startInt() {
    minInt = null;
  }

  @Override
  protected void startLong() {
    minLong = null;
  }

  @Override
  protected void startFloat() {
    minFloat = null;
  }

  @Override
  protected void startDouble() {
    minDouble = null;
  }

  @Override
  protected void operateOnInt(int current, StructuredRecord record) {
    minInt = minInt == null ? current : Math.min(minInt, current);
    if (Objects.equals(minInt, current)) {
      minRecord = record;
    }
  }

  @Override
  protected void operateOnLong(long current, StructuredRecord record) {
    minLong = minLong == null ? current : Math.min(minLong, current);
    if (Objects.equals(minLong, current)) {
      minRecord = record;
    }
  }

  @Override
  protected void operateOnFloat(float current, StructuredRecord record) {
    minFloat = minFloat == null ? current : Math.min(minFloat, current);
    if (Objects.equals(minFloat, current)) {
      minRecord = record;
    }
  }

  @Override
  protected void operateOnDouble(double current, StructuredRecord record) {
    minDouble = minDouble == null ? current : Math.min(minDouble, current);
    if (Objects.equals(minDouble, current)) {
      minRecord = record;
    }
  }

  @Override
  protected List<StructuredRecord> getRecords() {
    List<StructuredRecord> records = new ArrayList<>();
    if (minRecord != null) {
      records.add(minRecord);
    }
    return records;
  }
}
