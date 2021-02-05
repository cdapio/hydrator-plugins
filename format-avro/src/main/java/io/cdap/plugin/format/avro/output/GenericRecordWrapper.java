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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * TODO: add
 */
public class GenericRecordWrapper {
  private final GenericRecord genericRecord;
  private final Schema schema;
  private final int hash;

  public GenericRecordWrapper(GenericRecord genericRecord, Schema schema, int hash) {
    this.genericRecord = genericRecord;
    this.schema = schema;
    this.hash = hash;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public Schema getSchema() {
    return schema;
  }

  public int getHash() {
    return hash;
  }
}
