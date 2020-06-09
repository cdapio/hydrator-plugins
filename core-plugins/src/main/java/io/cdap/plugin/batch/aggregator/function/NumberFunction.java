/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Base class for number based aggregate functions.
 * Allows subclasses to implement typed methods instead of implementing their own casting logic.
 *
 * @param <V> type of aggregate function
 */
public abstract class NumberFunction<V extends NumberFunction> implements AggregateFunction<Number, V> {
  protected final String fieldName;
  protected final Schema fieldSchema;
  protected final Schema.Type fieldType;
  protected Number number;

  public NumberFunction(final String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    this.fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
  }

  @Override
  public void initialize() {
    this.number = null;
  }

  @Override
  public Number getAggregate() {
    return number;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }
}
