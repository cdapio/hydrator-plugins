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

package co.cask.plugin.etl.batch.commons;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>>Hive Schema Converter class to convert {@link Schema} to {@link HCatSchema} and vice versa.</p>
 * Note: If the {@link HCatSchema} contains non-primitive type then this conversion to {@link Schema} will fail.
 */
public class HiveSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaConverter.class);


  /**
   * Converts a CDAP's {@link Schema} to Hive's {@link HCatSchema}.
   *
   * @param schema the {@link Schema}
   * @return {@link HCatSchema} for the given {@link Schema}
   */
  public static HCatSchema toHiveSchema(Schema schema) {
    List<HCatFieldSchema> fields = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      try {
        fields.add(new HCatFieldSchema(name, getType(name, field.getSchema().getType()), ""));
      } catch (HCatException e) {
        LOG.error("Failed to create HCatFieldSchema field {} of type {} from schema", name,
                  field.getSchema().getType());
      }
    }
    return new HCatSchema(fields);
  }

  /**
   * Returns the {@link PrimitiveTypeInfo} for the {@link Schema.Type}
   *
   * @param name name of the field
   * @param category {@link Schema.Type} of the field
   * @return {@link PrimitiveTypeInfo} for the given {@link Schema.Type} which is compatible with Hive.
   */
  private static PrimitiveTypeInfo getType(String name, Schema.Type category) {
    switch (category) {
      case BOOLEAN:
        return TypeInfoFactory.booleanTypeInfo;
      case INT:
        return TypeInfoFactory.intTypeInfo;
      case LONG:
        return TypeInfoFactory.longTypeInfo;
      case FLOAT:
        return TypeInfoFactory.floatTypeInfo;
      case DOUBLE:
        return TypeInfoFactory.doubleTypeInfo;
      case STRING:
        return TypeInfoFactory.stringTypeInfo;
      case BYTES:
        return TypeInfoFactory.binaryTypeInfo;
      default:
        throw new IllegalArgumentException(String.format(
          "Schema contains field '%s' with unsupported type %s", name, category.name()));
    }
  }

  /**
   * <p>Converts a {@link HCatSchema} from hive to {@link Schema} for CDAP.</p>
   * <p><b>Note:</b> This conversion does not support non-primitive types and the conversion will fail.
   * The conversion might also change the primitive type.
   * See {@link #getType(String, PrimitiveObjectInspector.PrimitiveCategory)} for details.</p>
   *
   * @param hiveSchema the {@link HCatSchema} of the hive table
   * @return {@link Schema} for the given {@link HCatSchema}
   */
  public static Schema toSchema(HCatSchema hiveSchema) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (HCatFieldSchema field : hiveSchema.getFields()) {
      String name = field.getName();
      if (field.isComplex()) {
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with complex type %s. Only primitive types are supported.",
          name, field.getTypeString()));
      }
      fields.add(Schema.Field.of(name, Schema.of(getType(name, field.getTypeInfo().getPrimitiveCategory()))));
    }
    return Schema.recordOf("record", fields);
  }

  /**
   * Returns the {@link Schema.Type} compatible for this field from hive.
   *
   * @param name name of the field
   * @param category the field's {@link PrimitiveObjectInspector.PrimitiveCategory}
   * @return the {@link Schema.Type} for this field
   */
  private static Schema.Type getType(String name, PrimitiveObjectInspector.PrimitiveCategory category) {
    switch (category) {
      case BOOLEAN:
        return Schema.Type.BOOLEAN;
      case BYTE:
      case CHAR:
      case SHORT:
      case INT:
        return Schema.Type.INT;
      case LONG:
        return Schema.Type.LONG;
      case FLOAT:
        return Schema.Type.FLOAT;
      case DOUBLE:
        return Schema.Type.DOUBLE;
      case STRING:
      case VARCHAR:
        return Schema.Type.STRING;
      case BINARY:
        return Schema.Type.BYTES;
      case VOID:
      case DATE:
      case TIMESTAMP:
      case DECIMAL:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s",
                                                         name, category.name()));
    }
  }
}
