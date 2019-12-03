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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides Avro formatters.
 */
public class AvroInputProvider implements FileInputFormatterProvider {

  @Nullable
  @Override
  public Schema getSchema(@Nullable String pathField, String filePath) {
    return null;
  }

  @Override
  public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
    return new AvroInputFormatter(schema);
  }
}
