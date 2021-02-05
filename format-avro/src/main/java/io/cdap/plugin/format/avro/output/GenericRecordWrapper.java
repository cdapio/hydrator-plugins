/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.avro.output;

import org.apache.avro.generic.GenericRecord;

/**
 * Wrapped for Generic Record which includes the hash value for the Schema.
 */
public class GenericRecordWrapper {
  private final GenericRecord genericRecord;
  private final int schemaHash;

  public GenericRecordWrapper(GenericRecord genericRecord, int schemaHash) {
    this.genericRecord = genericRecord;
    this.schemaHash = schemaHash;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public int getSchemaHash() {
    return schemaHash;
  }
}
