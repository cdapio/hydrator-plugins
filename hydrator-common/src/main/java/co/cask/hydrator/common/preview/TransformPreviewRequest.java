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
import com.google.gson.JsonObject;

import java.io.IOException;

/**
 * Request for transform previews.
 *
 * @param <T> the PluginConfig
 */
public class TransformPreviewRequest<T> extends PreviewRequest<T> {
  private final Schema inputSchema;
  private final JsonObject inputRecord;

  public TransformPreviewRequest(Schema inputSchema, JsonObject inputRecord, T properties) {
    super(properties);
    this.inputSchema = inputSchema;
    this.inputRecord = inputRecord;
  }

  public Schema getInputSchema() {
    return inputSchema;
  }

  public StructuredRecord getInputStructuredRecord() throws IOException {
    return StructuredRecordStringConverter.fromJsonString(inputRecord.toString(), inputSchema);
  }
}
