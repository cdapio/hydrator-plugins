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

package co.cask.hydrator.plugin.batch.aggregator.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

/**
 *
 */
public class Concat implements AggregateFunction<String> {

  private final String fieldName;
  private final Schema outputSchema;
  private StringBuilder stringBuilder;
  private final String delimter = new String("|");


  public Concat(String fieldName, Schema fieldSchema) {
    this.stringBuilder = new StringBuilder();
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.STRING)) : Schema.of(Schema.Type.STRING);
  }

  @Override
  public String getAggregate() {
    return stringBuilder.toString();
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public void beginFunction() {
    stringBuilder.setLength(0);
  }

  @Override
  public void operateOn(StructuredRecord record) {
    Object object = record.get(fieldName);
    if (stringBuilder.length() != 0) {
      stringBuilder.append(delimter);
    }
    if (object instanceof  Long) {
      stringBuilder.append(String.valueOf((Long) object));
    } else if (object instanceof  Integer) {
      stringBuilder.append(String.valueOf((Integer) object));
    } else {
      stringBuilder.append((String) object);
    }
  }
}
