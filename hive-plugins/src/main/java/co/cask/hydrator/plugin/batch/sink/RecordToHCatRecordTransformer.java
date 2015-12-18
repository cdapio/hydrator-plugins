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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaConverter;
import com.google.common.base.Preconditions;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * A transform to convert a {@link StructuredRecord} to Hive's {@link HCatRecord}.
 */
public class RecordToHCatRecordTransformer {

  private final HCatSchema hCatSchema;
  private final Schema schema;

  /**
   * A transform to convert a {@link StructuredRecord} to Hive's {@link HCatRecord}. The given {@link Schema} and
   * {@link HCatSchema} must be compatible. To convert one schema to another and supported types
   * see {@link HiveSchemaConverter}
   */
  public RecordToHCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  /**
   * Converts a {@link StructuredRecord} to {@link HCatRecord} using the {@link #hCatSchema}.
   *
   * @param record {@link StructuredRecord} to be converted
   * @return {@link HCatRecord} for the given {@link StructuredRecord}
   * @throws HCatException if failed to set the field in {@link HCatRecord}
   */
  public HCatRecord toHCatRecord(StructuredRecord record) throws HCatException {

    HCatRecord hCatRecord = new DefaultHCatRecord(schema.getFields().size());

    for (Schema.Field field : schema.getFields()) {
      Preconditions.checkNotNull(record.getSchema().getField(field.getName()), "Missing schema field '%s' in record " +
        "to be written.", field.getName());
      hCatRecord.set(field.getName(), hCatSchema, record.get(field.getName()));
    }

    return hCatRecord;
  }
}
