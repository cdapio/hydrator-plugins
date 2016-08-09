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

import java.io.IOException;
import java.util.List;

/**
 * Creates ORCStruct records from StructuredRecords
 */
public class StructuredToOrcTransformer extends RecordConverter<StructuredRecord, OrcStruct> {

  private static final Logger LOG = LoggerFactory.getLogger(StructuredToOrcTransformer.class);

  @Override
  public OrcStruct transform(StructuredRecord input) throws IOException {
    StringBuilder builder = new StringBuilder();
    try {
      HiveSchemaConverter.appendType(builder, input.getSchema());
    } catch (UnsupportedTypeException e) {
      LOG.debug("Not a valid Schema {}", input.getSchema().toString(), e);
    }
    TypeDescription schema = TypeDescription.fromString(builder.toString());
    OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
    List<Schema.Field> fields = input.getSchema().getFields();

    //populate ORC struct Pair object
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      try {
        WritableComparable writable = convertToWritable(field, input);
        pair.setFieldValue(fields.get(i).getName(), writable);
      } catch (UnsupportedTypeException e) {
        LOG.debug(field.getName() + "is not a supported type", e);
      }
    }
    return pair;
  }

  private WritableComparable convertToWritable(Schema.Field field, StructuredRecord input)
    throws UnsupportedTypeException {
    WritableComparable writable = null;
    Object fieldVal = input.get(field.getName());
    switch (field.getSchema().getType()) {
      case NULL:
        break;
      case STRING:
      case ENUM:
        writable = new Text((String) fieldVal);
        break;
      case BOOLEAN:
        writable = new BooleanWritable((Boolean) fieldVal);
        break;
      case INT:
        writable = new IntWritable(Integer.valueOf((Integer) fieldVal));
        break;
      case LONG:
        writable = new LongWritable(Long.valueOf((Long) fieldVal));
        break;
      case FLOAT:
        writable = new FloatWritable((Float) fieldVal);
        break;
      case DOUBLE:
        writable = new DoubleWritable(Double.valueOf((Double) fieldVal));
        break;
      case BYTES:
        writable = new BytesWritable(((String) fieldVal).getBytes());
        break;
      default:
        throw new UnsupportedTypeException(field.getSchema().getType().name() +
                                             "type is currently not supported in ORC");
          }
    return writable;
  }
}
