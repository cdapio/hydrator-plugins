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
  protected Integer updateInt(int val) {
    intSum += val;
    return intSum;
  }

  @Override
  protected Long updateLong(long val) {
    longSum += val;
    return longSum;
  }

  @Override
  protected Float updateFloat(float val) {
    floatSum += val;
    return floatSum;
  }

  @Override
  protected Double updateDouble(double val) {
    doubleSum += val;
    return doubleSum;
  }

  @Override
  protected Integer finishInt() {
    return intSum;
  }

  @Override
  protected Long finishLong() {
    return longSum;
  }

  @Override
  protected Float finishFloat() {
    return floatSum;
  }

  @Override
  protected Double finishDouble() {
    return doubleSum;
  }
}
