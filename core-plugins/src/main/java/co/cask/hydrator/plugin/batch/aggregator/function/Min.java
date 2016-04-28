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
 * Calculates minimum values of a field in a group.
 */
public class Min extends NumberFunction {
  private Integer minInt;
  private Long minLong;
  private Float minFloat;
  private Double minDouble;

  public Min(String fieldName, @Nullable Schema fieldSchema) {
    super(fieldName, fieldSchema);
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
  protected void updateInt(int val) {
    minInt = minInt == null ? val : Math.min(minInt, val);
  }

  @Override
  protected void updateLong(long val) {
    minLong = minLong == null ? val : Math.min(minLong, val);
  }

  @Override
  protected void updateFloat(float val) {
    minFloat = minFloat == null ? val : Math.min(minFloat, val);
  }

  @Override
  protected void updateDouble(double val) {
    minDouble = minDouble == null ? val : Math.min(minDouble, val);
  }

  @Override
  protected Integer getInt() {
    return minInt;
  }

  @Override
  protected Long getLong() {
    return minLong;
  }

  @Override
  protected Float getFloat() {
    return minFloat;
  }

  @Override
  protected Double getDouble() {
    return minDouble;
  }
}
