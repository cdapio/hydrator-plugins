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

package co.cask.hydrator.common.preview;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;

/**
 *
 */
public class PreviewRecord {
  private static final Gson GSON = new Gson();
  private final Schema outputSchema;
  private final JsonObject outputRecord;

  private PreviewRecord(Schema schema, JsonObject record) {
    this.outputSchema = schema;
    this.outputRecord = record;
  }

  public static PreviewRecord from(StructuredRecord record) throws IOException {
    return new PreviewRecord(record.getSchema(),
                             GSON.fromJson(StructuredRecordStringConverter.toJsonString(record), JsonObject.class));
  }
}
