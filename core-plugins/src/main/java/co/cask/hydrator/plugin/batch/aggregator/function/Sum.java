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

/**
 * Performs a sum on a field.
 */
public class Sum extends NumberFunction {
  private int intSum;
  private long longSum;
  private double doubleSum;
  private float floatSum;

  public Sum(String fieldName, Schema fieldSchema) {
    super(fieldName, fieldSchema);
  }

  @Override
  protected void startInt() {
    intSum = 0;
  }

  @Override
  protected void startLong() {
    longSum = 0;
  }

  @Override
  protected void startFloat() {
    floatSum = 0;
  }

  @Override
  protected void startDouble() {
    doubleSum = 0;
  }

  @Override
  protected void updateInt(int val) {
    intSum += val;
  }

  @Override
  protected void updateLong(long val) {
    longSum += val;
  }

  @Override
  protected void updateFloat(float val) {
    floatSum += val;
  }

  @Override
  protected void updateDouble(double val) {
    doubleSum += val;
  }

  @Override
  protected Integer getInt() {
    return intSum;
  }

  @Override
  protected Long getLong() {
    return longSum;
  }

  @Override
  protected Float getFloat() {
    return floatSum;
  }

  @Override
  protected Double getDouble() {
    return doubleSum;
  }
}
