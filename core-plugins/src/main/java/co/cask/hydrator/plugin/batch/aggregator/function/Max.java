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

import co.cask.cdap.api.data.schema.Schema;

import javax.annotation.Nullable;

/**
 * Calculates max values of a field in a group.
 */
public class Max extends NumberFunction {
  private Integer maxInt;
  private Long maxLong;
  private Float maxFloat;
  private Double maxDouble;

  public Max(String fieldName, @Nullable Schema fieldSchema) {
    super(fieldName, fieldSchema);
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
  protected void updateInt(int val) {
    maxInt = maxInt == null ? val : Math.max(maxInt, val);
  }

  @Override
  protected void updateLong(long val) {
    maxLong = maxLong == null ? val : Math.max(maxLong, val);
  }

  @Override
  protected void updateFloat(float val) {
    maxFloat = maxFloat == null ? val : Math.max(maxFloat, val);
  }

  @Override
  protected void updateDouble(double val) {
    maxDouble = maxDouble == null ? val : Math.max(maxDouble, val);
  }

  @Override
  protected Integer getInt() {
    return maxInt;
  }

  @Override
  protected Long getLong() {
    return maxLong;
  }

  @Override
  protected Float getFloat() {
    return maxFloat;
  }

  @Override
  protected Double getDouble() {
    return maxDouble;
  }
}
