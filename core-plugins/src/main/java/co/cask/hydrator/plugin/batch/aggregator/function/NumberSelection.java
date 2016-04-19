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
 * if {@link #operateOn(StructuredRecord)} is called, only {@link #operateOnInt(StructuredRecord)} will be called until
 * {@link #finishInt()} is called.
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
          operateOnDouble(record);
        }

        @Override
        public void finishFunction() {
          finishDouble();
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
            operateOnInt(record);
          }

          @Override
          public void finishFunction() {
            finishInt();
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
            operateOnLong(record);
          }

          @Override
          public void finishFunction() {
            finishLong();
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
            operateOnFloat(record);
          }

          @Override
          public void finishFunction() {
            finishFloat();
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
            operateOnDouble(record);
          }

          @Override
          public void finishFunction() {
            finishDouble();
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
  public void finishFunction() {
    delegate.finishFunction();
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

  protected abstract void operateOnInt(StructuredRecord record);

  protected abstract void operateOnLong(StructuredRecord record);

  protected abstract void operateOnFloat(StructuredRecord record);

  protected abstract void operateOnDouble(StructuredRecord record);

  protected void finishDouble() {
    // no-op
  }

  protected void finishInt() {
    // no-op
  }

  protected void finishLong() {
    // no-op
  }

  protected void finishFloat() {
    // no-op
  }

  protected abstract List<StructuredRecord> getRecords();
}
