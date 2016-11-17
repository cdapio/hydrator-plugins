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
package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.SparseVector;

import java.util.List;

/**
 * Vector utility class to handle sparse vectors.
 */
public class VectorUtils {
  public static final Schema SPARSE_SCHEMA =
    Schema.recordOf("sparseVector", Schema.Field.of("size", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("indices", Schema.arrayOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("vectorValues", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));

  public static StructuredRecord asRecord(SparseVector vector) {
    return StructuredRecord.builder(SPARSE_SCHEMA)
      .set("size", vector.size())
      .set("indices", vector.indices())
      .set("vectorValues", vector.values())
      .build();
  }

  public static SparseVector fromRecord(StructuredRecord record) {
    int size = record.get("size");
    Object index = record.get("indices");
    int[] indices;
    if (index instanceof List) {
      List<Integer> indexList = (List<Integer>) index;
      indices = ArrayUtils.toPrimitive(indexList.toArray(new Integer[indexList.size()]));
    } else {
      indices = (int[]) index;
    }
    Object value = record.get("vectorValues");
    double[] values;
    if (value instanceof List) {
      List<Double> doubleList = (List<Double>) value;
      values = ArrayUtils.toPrimitive(doubleList.toArray(new Double[doubleList.size()]));
    } else {
      values = (double[]) value;
    }
    return new SparseVector(size, indices, values);
  }
}
