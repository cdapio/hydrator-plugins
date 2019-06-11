/*
 * Copyright © 2019 Cask Data, Inc.
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


package co.cask.hydrator.plugin.batch.aggregator.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Collect List of a specific column
 * @param <T> type of aggregate value
 */
public class CollectList<T> implements AggregateFunction<List<T>> { 
  private List<T> collectList;
  private final String fieldName;
  private final Schema fieldSchema;

  public CollectList(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
  }

  @Override
  public void beginFunction() {
    collectList = new ArrayList<>();
  }

  @Override
  public void operateOn(StructuredRecord record) {
    T field = record.get(fieldName);
    collectList.add(field);
  }

  @Override
  public List<T> getAggregate() {
    return collectList;
  }

  @Override
  public Schema getOutputSchema() {
    return Schema.arrayOf(fieldSchema);
  }
}
