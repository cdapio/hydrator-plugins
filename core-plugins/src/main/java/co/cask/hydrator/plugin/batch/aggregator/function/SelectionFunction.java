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

import java.util.List;

/**
 * Peforms selection. To perform selection on a group of {@link StructuredRecord}, {@link #beginFunction} is first
 * called, followed by calls to {@link #operateOn}. Finally the {@link #finishFunction} should be invoked before getting
 * the selected records from {@link #getSelectedRecords}.
 */
public interface SelectionFunction extends RecordFunctionLifecycle {

  /**
   * @return {@link StructuredRecord} that is chosen based on the aggregate function.
   */
  List<StructuredRecord> getSelectedRecords();
}
