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

import java.io.Serializable;
import java.util.Objects;

/**
 * A key that hashes and is equal based on a single key value, but which also contains the start date for ordering.
 */
public class SCD2Key<T extends Comparable<T>> implements Serializable {
  private final T key;
  private final int startDate;

  public SCD2Key(T key, int startDate) {
    this.key = key;
    this.startDate = startDate;
  }

  public T getKey() {
    return key;
  }

  public int getStartDate() {
    return startDate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SCD2Key other = (SCD2Key) o;
    return Objects.equals(key, other.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }
}
