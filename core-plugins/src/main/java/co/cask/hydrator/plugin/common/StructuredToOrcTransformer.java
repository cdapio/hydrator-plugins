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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.common.RecordConverter;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Creates ORCStruct records from StructuredRecords
 */
public class StructuredToOrcTransformer extends RecordConverter<StructuredRecord, OrcStruct> {

  private static final Logger LOG = LoggerFactory.getLogger(StructuredToOrcTransformer.class);

  @Override
  public OrcStruct transform(StructuredRecord input) {

    List<Schema.Field> fields = input.getSchema().getFields();
    OrcStruct pair = parseOrcSchema(input.getSchema());
    //populate ORC struct Pair object
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      try {
        WritableComparable writable = convertToWritable(field, input);
        pair.setFieldValue(fields.get(i).getName(), writable);
      } catch (UnsupportedTypeException e) {
        LOG.debug("{} is not a supported type", field.getName(), e);
        throw new IllegalArgumentException(String.format("{} is not a supported type", field.getName()), e);
      }
    }
    return pair;
  }

  public OrcStruct parseOrcSchema(Schema inputSchema) {
    StringBuilder builder = new StringBuilder();
    try {
      HiveSchemaConverter.appendType(builder, inputSchema);
    } catch (UnsupportedTypeException e) {
      LOG.debug("Not a valid Schema {}", inputSchema.toString(), e);
    }
    TypeDescription schema = TypeDescription.fromString(builder.toString());
    OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
    return pair;
  }

  private WritableComparable convertToWritable(Schema.Field field, StructuredRecord input)
    throws UnsupportedTypeException {
    Object fieldVal = input.get(field.getName());
    switch (field.getSchema().getType()) {
      case NULL:
        break;
      case STRING:
      case ENUM:
        return new Text((String) fieldVal);
      case BOOLEAN:
        return new BooleanWritable((Boolean) fieldVal);
      case INT:
        return new IntWritable((Integer) fieldVal);
      case LONG:
        return new LongWritable((Long) fieldVal);
      case FLOAT:
        return new FloatWritable((Float) fieldVal);
      case DOUBLE:
        return new DoubleWritable((Double) fieldVal);
      case BYTES:
        return new BytesWritable(((String) fieldVal).getBytes(StandardCharsets.UTF_8));
      default:
        throw new UnsupportedTypeException(String.format("{} type is currently not supported in ORC",
                                                         field.getSchema().getType().name()));
          }
          return null;
  }
}
