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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Concatenates only distinct values in the group with a comma
 */
public class ConcatDistinct implements AggregateFunction<String, ConcatDistinct> {

  private final String fieldName;
  private final Schema fieldSchema;
  private Set<String> values;

  public ConcatDistinct(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    Type inputType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (!inputType.equals(Type.STRING)) {
      throw new IllegalArgumentException(
        String.format("Field '%s' is of unsupported non-string type '%s'. ", fieldName, inputType));
    }
  }

  @Override
  public void initialize() {
    values = new LinkedHashSet<>();
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    if (record.get(fieldName) != null) {
      String value = record.get(fieldName);
      values.add(value);
    }
  }

  @Override
  public void mergeAggregates(ConcatDistinct otherAgg) {
    values.addAll(otherAgg.values);
  }

  @Override
  public String getAggregate() {
    return String.join(", ", values);
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

}
