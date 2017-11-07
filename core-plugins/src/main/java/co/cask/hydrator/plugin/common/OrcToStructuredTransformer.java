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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Creates ORCStruct records from StructuredRecords
 */
public class OrcToStructuredTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(OrcToStructuredTransformer.class);
  private final Schema schema;

  public OrcToStructuredTransformer(Schema schema) {
    this.schema = schema;
  }

  public StructuredRecord transform(OrcStruct record) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema.Type type = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() :
        field.getSchema().getType();
      switch (type) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case ENUM:
        case BYTES: {
          try {
            builder.set(fieldName, getSchemaCompatibleValue(record, fieldName));
            break;
          } catch (Throwable t) {
            throw new RuntimeException(String.format("Error converting field '%s' of type %s",
                                                     fieldName, type), t);
          }
        }
        default: {
          throw new IllegalStateException(String.format("Output schema contains field '%s' with unsupported type %s.",
                                                        fieldName, type));
        }
      }
    }

    return builder.build();
  }

  private Object getSchemaCompatibleValue(OrcStruct record, String fieldName) {
    Class<? extends WritableComparable> type = record.getFieldValue(fieldName).getClass();
    LOG.info("### class is {}", type);
    if (type == Text.class) {
      return record.getFieldValue(fieldName).toString();
    } else if (type == LongWritable.class) {
      return ((LongWritable) record.getFieldValue(fieldName)).get();
    } else if (type == IntWritable.class) {
      return ((IntWritable) record.getFieldValue(fieldName)).get();
    } else if (type == BooleanWritable.class) {
      return ((BooleanWritable) record.getFieldValue(fieldName)).get();
    } else if (type == FloatWritable.class) {
      return ((FloatWritable) record.getFieldValue(fieldName)).get();
    } else if (type == DoubleWritable.class) {
      return ((DoubleWritable) record.getFieldValue(fieldName)).get();
    } else if (type == BytesWritable.class) {
      return ((BytesWritable) record.getFieldValue(fieldName)).getBytes();
    } else {
      throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s. " +
                                                         "To read this table you should provide input schema in " +
                                                         "which this field is dropped.", fieldName, type));
    }
  }
}
