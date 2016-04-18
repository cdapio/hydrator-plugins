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
 * if {@link #updateInt(int)} is called, only {@link #updateInt(int)} will be called until {@link #finishInt()}
 * is called.
 */
public abstract class NumberFunction implements RecordAggregateFunction<Number> {
  private final AggregateFunction<? extends Number> typedDelegate;
  private StructuredRecord chosenRecord;

  public NumberFunction(final String fieldName, @Nullable Schema fieldSchema) {
    // if schema is not known before we start getting records, just use doubles.
    if (fieldSchema == null) {
      typedDelegate = new AggregateFunction<Double>() {
        @Override
        public void beginAggregate() {
          startDouble();
        }

        @Override
        public void update(StructuredRecord record) {
          Number val = record.get(fieldName);
          if (val != null) {
            Double updateDouble = updateDouble(val.doubleValue());
            if (val.doubleValue() == updateDouble) {
              chosenRecord = record;
            }
          }
        }

        @Override
        public Double finishAggregate() {
          return finishDouble();
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
          public void beginAggregate() {
            startInt();
          }

          @Override
          public void update(StructuredRecord record) {
            Integer val = record.get(fieldName);
            if (val != null) {
              Integer updatedInt = updateInt(val);
              if (val.intValue() == updatedInt) {
                chosenRecord = record;
              }
            }
          }

          @Override
          public Integer finishAggregate() {
            return finishInt();
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
          public void beginAggregate() {
            startLong();
          }

          @Override
          public void update(StructuredRecord record) {
            Long val = record.get(fieldName);
            if (val != null) {
              Long updatedLong = updateLong(val);
              if (val.longValue() == updatedLong) {
                chosenRecord = record;
              }
            }
          }

          @Override
          public Long finishAggregate() {
            return finishLong();
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
          public void beginAggregate() {
            startFloat();
          }

          @Override
          public void update(StructuredRecord record) {
            Float val = record.get(fieldName);
            if (val != null) {
              Float updatedFloat = updateFloat(val);
              if (val.floatValue() == updatedFloat) {
                chosenRecord = record;
              }
            }
          }

          @Override
          public Float finishAggregate() {
            return finishFloat();
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
          public void beginAggregate() {
            startDouble();
          }

          @Override
          public void update(StructuredRecord record) {
            Double val = record.get(fieldName);
            if (val != null) {
              Double updatedDouble = updateDouble(val);
              if (val.doubleValue() == updatedDouble) {
                chosenRecord = record;
              }
            }
          }

          @Override
          public Double finishAggregate() {
            return finishDouble();
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
  public void beginAggregate() {
    typedDelegate.beginAggregate();
  }

  @Override
  public void update(StructuredRecord record) {
    typedDelegate.update(record);
  }

  @Override
  public Number finishAggregate() {
    return typedDelegate.finishAggregate();
  }

  @Override
  public Schema getOutputSchema() {
    return typedDelegate.getOutputSchema();
  }

  protected abstract void startInt();

  protected abstract void startLong();

  protected abstract void startFloat();

  protected abstract void startDouble();

  protected abstract Integer updateInt(int val);

  protected abstract Long updateLong(long val);

  protected abstract Float updateFloat(float val);

  protected abstract Double updateDouble(double val);

  @Nullable
  protected abstract Integer finishInt();

  @Nullable
  protected abstract Long finishLong();

  @Nullable
  protected abstract Float finishFloat();

  @Nullable
  protected abstract Double finishDouble();

  @Override
  @Nullable
  public StructuredRecord getChosenRecord() {
    return chosenRecord;
  }
}
