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
import co.cask.cdap.etl.api.Aggregator;

/**
 * Methods to manage lifecycle functions performed on group of {@link StructuredRecord}, typically used in the
 * {@link Aggregator#aggregate} stage.
 */
public interface RecordFunctionLifecycle {

  /**
   * Called once to start the function.
   */
  void beginFunction();

  /**
   * Called once for each {@link StructuredRecord} to operate up on. Calls to this method are made after the call to
   * {@link #beginFunction}
   * @param record {@link StructuredRecord} input record
   */
  void operateOn(StructuredRecord record);
}
