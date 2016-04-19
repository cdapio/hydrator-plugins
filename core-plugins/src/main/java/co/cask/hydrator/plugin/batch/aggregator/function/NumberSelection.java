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

import java.util.List;
import javax.annotation.Nullable;

/**
 * Base class for number based selection functions.
 * Allows subclasses to implement typed methods instead of implementing their own casting logic.
 * Guarantees that only methods for one type will be called for each aggregate. For example,
 * if {@link #operateOn(StructuredRecord)} is called, only {@link #operateOnInt(int, StructuredRecord)} will be called.
 */
public abstract class NumberSelection implements SelectionFunction {
  private final SelectionFunction delegate;
  private final String fieldName;

  public NumberSelection(final String fieldName, @Nullable Schema fieldSchema) {
    this.fieldName = fieldName;
    // if schema is not known before we start getting records, just use doubles.
    if (fieldSchema == null) {
      delegate = new SelectionFunction() {
        @Override
        public List<StructuredRecord> getSelectedRecords() {
          return getRecords();
        }

        @Override
        public void beginFunction() {
          startDouble();
        }

        @Override
        public void operateOn(StructuredRecord record) {
          Number number = record.get(fieldName);
          if (number != null) {
            operateOnDouble(number.doubleValue(), record);
          }
        }
      };
      return;
    }

    final boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case INT:
        delegate = new SelectionFunction() {
          @Override
          public List<StructuredRecord> getSelectedRecords() {
            return getRecords();
          }

          @Override
          public void beginFunction() {
            startInt();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Integer value = record.get(fieldName);
            if (value != null) {
              operateOnInt(value, record);
            }
          }
        };
        break;
      case LONG:
        delegate = new SelectionFunction() {
          @Override
          public List<StructuredRecord> getSelectedRecords() {
            return getRecords();
          }

          @Override
          public void beginFunction() {
            startLong();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Long value = record.get(fieldName);
            if (value != null) {
              operateOnLong(value, record);
            }
          }
        };
        break;
      case FLOAT:
        delegate = new SelectionFunction() {
          @Override
          public List<StructuredRecord> getSelectedRecords() {
            return getRecords();
          }

          @Override
          public void beginFunction() {
            startFloat();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Float value = record.get(fieldName);
            if (value != null) {
              operateOnFloat(value, record);
            }
          }
        };
        break;
      case DOUBLE:
        delegate = new SelectionFunction() {
          @Override
          public List<StructuredRecord> getSelectedRecords() {
            return getRecords();
          }

          @Override
          public void beginFunction() {
            startDouble();
          }

          @Override
          public void operateOn(StructuredRecord record) {
            Double value = record.get(fieldName);
            if (value != null) {
              operateOnDouble(value, record);
            }
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
    delegate.beginFunction();
  }

  @Override
  public void operateOn(StructuredRecord record) {
    delegate.operateOn(record);
  }

  @Override
  public List<StructuredRecord> getSelectedRecords() {
    return delegate.getSelectedRecords();
  }

  public String getFieldName() {
    return fieldName;
  }

  protected abstract void startInt();

  protected abstract void startLong();

  protected abstract void startFloat();

  protected abstract void startDouble();

  protected abstract void operateOnInt(int val, StructuredRecord record);

  protected abstract void operateOnLong(long val, StructuredRecord record);

  protected abstract void operateOnFloat(float val, StructuredRecord record);

  protected abstract void operateOnDouble(double val, StructuredRecord record);

  protected abstract List<StructuredRecord> getRecords();
}
