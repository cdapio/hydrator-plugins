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

import javax.annotation.Nullable;

/**
 * Base class for number based aggregate functions.
 * Allows subclasses to implement typed methods instead of implementing their own casting logic.
 * Guarantees that only methods for one type will be called for each aggregate. For example,
 * if {@link #updateInt(int)} is called, only {@link #updateInt(int)} will be called.
 */
public abstract class NumberFunction implements AggregateFunction<Number> {
  private final AggregateFunction<? extends Number> typedDelegate;

  public NumberFunction(final String fieldName, @Nullable Schema fieldSchema) {
    // if schema is not known before we start getting records, just use doubles.
    if (fieldSchema == null) {
      typedDelegate = new AggregateFunction<Double>() {
        @Override
        public void beginFunction() {
          startDouble();
        }

        @Override
        public void operateOn(StructuredRecord record) {
          Number val = record.get(fieldName);
          if (val != null) {
            updateDouble(val.doubleValue());
          }
        }

        @Override
        public Double getAggregate() {
          return getDouble();
        }

        @Override
        public Schema getOutputSchema() {
          return Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
        }
      };
      return;
    }

    final boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case INT:
        typedDelegate = new AggregateFunction<Integer>() {
          @Override
          public void beginFunction() {
            startInt();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Integer val = record.get(fieldName);
            if (val != null) {
              updateInt(val);
            }
          }

          @Override
          public Integer getAggregate() {
            return getInt();
          }

          @Override
          public Schema getOutputSchema() {
            return isNullable ? Schema.nullableOf(Schema.of(Schema.Type.INT)) : Schema.of(Schema.Type.INT);
          }
        };
        break;
      case LONG:
        typedDelegate = new AggregateFunction<Long>() {
          @Override
          public void beginFunction() {
            startLong();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Long val = record.get(fieldName);
            if (val != null) {
              updateLong(val);
            }
          }

          @Override
          public Long getAggregate() {
            return getLong();
          }

          @Override
          public Schema getOutputSchema() {
            return isNullable ? Schema.nullableOf(Schema.of(Schema.Type.LONG)) : Schema.of(Schema.Type.LONG);
          }
        };
        break;
      case FLOAT:
        typedDelegate = new AggregateFunction<Float>() {
          @Override
          public void beginFunction() {
            startFloat();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Float val = record.get(fieldName);
            if (val != null) {
              updateFloat(val);
            }
          }

          @Override
          public Float getAggregate() {
            return getFloat();
          }

          @Override
          public Schema getOutputSchema() {
            return isNullable ? Schema.nullableOf(Schema.of(Schema.Type.FLOAT)) : Schema.of(Schema.Type.FLOAT);
          }
        };
        break;
      case DOUBLE:
        typedDelegate = new AggregateFunction<Double>() {
          @Override
          public void beginFunction() {
            startDouble();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Double val = record.get(fieldName);
            if (val != null) {
              updateDouble(val);
            }
          }

          @Override
          public Double getAggregate() {
            return getDouble();
          }

          @Override
          public Schema getOutputSchema() {
            return isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
          }
        };
        break;
      default:
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                         fieldName, fieldType));
    }
  }

  @Override
  public void beginFunction() {
    typedDelegate.beginFunction();
  }

  @Override
  public void operateOn(StructuredRecord record) {
    typedDelegate.operateOn(record);
  }

  @Override
  public Number getAggregate() {
    return typedDelegate.getAggregate();
  }

  @Override
  public Schema getOutputSchema() {
    return typedDelegate.getOutputSchema();
  }

  protected abstract void startInt();

  protected abstract void startLong();

  protected abstract void startFloat();

  protected abstract void startDouble();

  protected abstract void updateInt(int val);

  protected abstract void updateLong(long val);

  protected abstract void updateFloat(float val);

  protected abstract void updateDouble(double val);

  @Nullable
  protected abstract Integer getInt();

  @Nullable
  protected abstract Long getLong();

  @Nullable
  protected abstract Float getFloat();

  @Nullable
  protected abstract Double getDouble();
}
