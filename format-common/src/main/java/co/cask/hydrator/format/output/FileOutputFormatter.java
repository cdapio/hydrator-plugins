/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.output;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;

import java.io.IOException;
import java.util.Map;

/**
 * Defines how to prepare a job to write using a Hadoop OutputFormat as well as performing the logic to transform
 * a {@link StructuredRecord} into the object required by the OutputFormat.
 *
 * @param <K> the type of output key
 * @param <V> the type of output value
 */
public interface FileOutputFormatter<K, V> {

  /**
   * Get the class name for the Hadoop OutputFormat that will be used to write output.
   */
  String getFormatClassName();

  /**
   * Get the configuration required by the Hadoop OutputFormat.
   */
  Map<String, String> getFormatConfig();

  /**
   * Transform the record into the output key value expected by the output format.
   *
   * @param record the record to transform
   * @return the output key value expected by the output format
   * @throws IOException if there was an exception transforming the record
   */
  KeyValue<K, V> transform(StructuredRecord record) throws IOException;
}
