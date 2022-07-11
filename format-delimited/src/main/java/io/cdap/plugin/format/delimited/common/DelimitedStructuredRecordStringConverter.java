/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.SchemaValidator;

/**
 * StructuredRecordStringConverter implementation with additional logic for the delimited format
 */
public class DelimitedStructuredRecordStringConverter extends StructuredRecordStringConverter {

  /**
   * Set field value in the supplied Structured Record Builder.
   *
   * @param builder Structured Record Builder instance
   * @param field   field to set
   * @param part    String portion of the delimited input
   */
  public static void parseAndSetFieldValue(StructuredRecord.Builder builder,
                                           Schema.Field field,
                                           String part) {
    if (part.isEmpty()) {
      builder.set(field.getName(), null);
    } else {
      // Ensure if date time field, value is in correct format
      SchemaValidator.validateDateTimeField(field.getSchema(), field.getName(), part);

      // Call superclass for field conversion logic
      StructuredRecordStringConverter.handleFieldConversion(builder, field, part);
    }
  }

  protected DelimitedStructuredRecordStringConverter() {
  }
}
