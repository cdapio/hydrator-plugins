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

package io.cdap.plugin.common.db.dbrecordreader;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for lazy field initialization. Implementation is thread-safe.
 *
 * @param <T> value type
 */
public final class Lazy<T> {
  private final Supplier<T> supplier;
  private volatile T value;

  public Lazy(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  public T getOrCompute() {
    final T result = value; // Just one volatile read
    return result == null ? compute() : result;
  }

  private synchronized T compute() {
    if (value == null) {
      value = requireNonNull(supplier.get());
    }
    return value;
  }
}
