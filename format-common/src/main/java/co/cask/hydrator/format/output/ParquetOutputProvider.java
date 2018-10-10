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

import co.cask.cdap.api.data.schema.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates ParquetOutputFormatters.
 */
public class ParquetOutputProvider implements FileOutputFormatterProvider<Void, GenericRecord> {

  @Override
  public FileOutputFormatter<Void, GenericRecord> create(Map<String, String> properties,
                                                                          @Nullable Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be provided when writing as parquet.");
    }
    return new ParquetOutputFormatter(schema);
  }
}
