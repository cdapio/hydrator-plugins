
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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.io.IOException;
import java.util.Iterator;

/**
 * Creates Single String from StructuredRecord
 */
public class StructuredToTextTransformer {
  private final String delimiter;

  public StructuredToTextTransformer(String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * appends the structured record fields as strings
   * separated by a delimiter. if the fields value is null they are empty strings.
   * returns concatenated string representing the record.
   * @param structuredRecord
   * @return String
   * @throws IOException
   */
  public String transform(StructuredRecord structuredRecord) throws IOException {
    StringBuilder builder = new StringBuilder();
    Iterator<Schema.Field> fieldIterator = structuredRecord.getSchema().getFields().iterator();
    if (fieldIterator.hasNext()) {
      builder.append(getFieldAsString(structuredRecord.get(fieldIterator.next().getName())));
    }

    while (fieldIterator.hasNext()) {
      builder.append(delimiter);
      builder.append(getFieldAsString(structuredRecord.get(fieldIterator.next().getName())));
    }
    return builder.toString();
  }

  private String getFieldAsString(Object field) {
    return field == null ? "" : field.toString();
  }
}
