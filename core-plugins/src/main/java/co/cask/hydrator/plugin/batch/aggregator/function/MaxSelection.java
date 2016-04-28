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
 * A {@link SelectionFunction} that can be used to select the record with the max value of a given field.
 */
public class MaxSelection extends NumberSelection {
  private StructuredRecord maxRecord;
  private Integer maxInt;
  private Long maxLong;
  private Float maxFloat;
  private Double maxDouble;

  public MaxSelection(String fieldName, Schema fieldSchema) {
    super(fieldName, fieldSchema);
    maxRecord = null;
  }

  @Override
  protected void startInt() {
    maxInt = null;
  }

  @Override
  protected void startLong() {
    maxLong = null;
  }

  @Override
  protected void startFloat() {
    maxFloat = null;
  }

  @Override
  protected void startDouble() {
    maxDouble = null;
  }

  @Override
  protected void operateOnInt(int current, StructuredRecord record) {
    maxInt = maxInt == null ? current : Math.max(maxInt, current);
    if (Objects.equals(maxInt, current)) {
      maxRecord = record;
    }
  }

  @Override
  protected void operateOnLong(long current, StructuredRecord record) {
    maxLong = maxLong == null ? current : Math.max(maxLong, current);
    if (Objects.equals(maxLong, current)) {
      maxRecord = record;
    }
  }

  @Override
  protected void operateOnFloat(float current, StructuredRecord record) {
    maxFloat = maxFloat == null ? current : Math.max(maxFloat, current);
    if (Objects.equals(maxFloat, current)) {
      maxRecord = record;
    }
  }

  @Override
  protected void operateOnDouble(double current, StructuredRecord record) {
    maxDouble = maxDouble == null ? current : Math.max(maxDouble, current);
    if (Objects.equals(maxDouble, current)) {
      maxRecord = record;
    }
  }

  @Override
  protected List<StructuredRecord> getRecords() {
    List<StructuredRecord> records = new ArrayList<>();
    if (maxRecord != null) {
      records.add(maxRecord);
    }
    return records;
  }
}
