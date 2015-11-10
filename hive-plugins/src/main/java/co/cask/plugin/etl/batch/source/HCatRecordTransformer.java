/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.plugin.etl.batch.commons.HiveSchemaConverter;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * A transform to convert a {@link HCatRecord} from hive to {@link StructuredRecord}.
 */
public class HCatRecordTransformer {
  private final HCatSchema hCatSchema;
  private final Schema schema;

  /**
   * A transform to convert a {@link HCatRecord} to Hive's {@link StructuredRecord}. The given {@link Schema} and
   * {@link HCatSchema} must be compatible. To convert one schema to another and supported types
   * see {@link HiveSchemaConverter}
   */
  public HCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  /**
   * Converts a {@link HCatRecord} read from a hive table to {@link StructuredRecord} using the {@link Schema} created
   * from the {@link HCatSchema}.
   *
   * @param hCatRecord the record
   * @return the converted {@link StructuredRecord}
   */
  public StructuredRecord toRecord(HCatRecord hCatRecord) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema.Type type = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() :
        field.getSchema().getType();
      switch (type) {
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BYTES: {
          try {
            builder.set(fieldName, hCatRecord.get(fieldName, hCatSchema));
            break;
          } catch (Throwable t) {
            throw new RuntimeException(String.format("Error converting field '%s' of type %s",
                                                     fieldName, field.getSchema().getType()), t);
          }
        }
        default: {
          throw new IllegalStateException(String.format("Output schema contains field '%s' with unsupported type %s.",
                                                        fieldName, field.getSchema().getType().name()));
        }
      }
    }
    return builder.build();
  }
}
