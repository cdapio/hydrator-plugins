/*
 * Copyright © 2019 Cask Data, Inc.
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

package org.apache.parquet.avro;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * A converter that doesn't do anything. This is used when the read schema does not contain all the fields that
 * are in the write schema.
 */
public class NoOpConverter extends PrimitiveConverter {
  public static final NoOpConverter INSTANCE = new NoOpConverter();

  private NoOpConverter() {
    // singleton
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    // no-op
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    // no-op
  }

  @Override
  public void addBinary(Binary value) {
    // no-op
  }

  @Override
  public void addBoolean(boolean value) {
    // no-op
  }

  @Override
  public void addDouble(double value) {
    // no-op
  }

  @Override
  public void addFloat(float value) {
    // no-op
  }

  @Override
  public void addInt(int value) {
    // no-op
  }

  @Override
  public void addLong(long value) {
    // no-op
  }
}
