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

import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 *
 */
public class SCD2Record {
  private final StructuredRecord prev;
  private final StructuredRecord cur;
  private final AtomicInteger latestDate;

  public SCD2Record(@Nullable StructuredRecord prev, StructuredRecord cur, AtomicInteger latestDate) {
    this.prev = prev;
    this.cur = cur;
    this.latestDate = latestDate;
  }

  @Nullable
  public StructuredRecord getPrev() {
    return prev;
  }

  public StructuredRecord getCur() {
    return cur;
  }

  public AtomicInteger getLatestDate() {
    return latestDate;
  }
}
