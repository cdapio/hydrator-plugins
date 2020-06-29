/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator.function;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Type;
import java.util.Objects;

/**
 * Concatenates only distinct values in the group with a comma
 */
public class ConcatDistinct implements AggregateFunction<String, ConcatDistinct> {

  private final String fieldName;
  private final Schema fieldSchema;
  private String concatString;
  private boolean firstString = true;

  public ConcatDistinct(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    Type inputType =
        fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (!inputType.equals(Type.STRING)) {
      throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported non-string type '%s'. ",
              fieldName, inputType));
    }
  }

  @Override
  public void initialize() {
    concatString = "";
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    if (record.get(fieldName) != null) {
      String value = Objects.requireNonNull(record.get(fieldName)).toString();
      if (!concatString.contains(value)) {
        if (firstString) {
          concatString = value;
          firstString = false;
          return;
        }
        concatString = String.format("%s, %s", concatString, value);
      }
    }
  }

  @Override
  public void mergeAggregates(ConcatDistinct otherAgg) {
    if (Strings.isNullOrEmpty(otherAgg.getAggregate())) {
      return;
    }
    if (concatString.equals("")) {
      concatString = otherAgg.getAggregate();
      return;
    }
    if (!concatString.contains(otherAgg.concatString)) {
      concatString = String.format("%s, %s", concatString, otherAgg.concatString);
    }
  }

  @Override
  public String getAggregate() {
    return concatString;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

}
