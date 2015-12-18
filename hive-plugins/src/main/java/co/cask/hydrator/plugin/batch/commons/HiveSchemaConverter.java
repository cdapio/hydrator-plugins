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

package co.cask.hydrator.plugin.batch.commons;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Preconditions;
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
   * Converts a CDAP's {@link Schema} to Hive's {@link HCatSchema} while verifying the fields in the given
   * {@link Schema} to exists in the table. The valid types for {@link Schema} which can be converted into
   * {@link HCatSchema} are boolean, int, long, float, double, string and bytes.
   *
   * @param schema the {@link Schema}
   * @return {@link HCatSchema} for the given {@link Schema}
   * @throws NullPointerException if a field in the given {@link Schema} is not found in table's {@link HCatSchema}
   */
  public static HCatSchema toHiveSchema(Schema schema, HCatSchema tableSchema) {
    List<HCatFieldSchema> fields = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      try {
        // this field of the schema must exist in the table and should be of the same type
        HCatFieldSchema hCatFieldSchema = tableSchema.get(name);
        Preconditions.checkNotNull(hCatFieldSchema, "Missing field %s in table schema", name);
        PrimitiveTypeInfo hiveType = hCatFieldSchema.getTypeInfo();
        PrimitiveTypeInfo type = getType(name, field.getSchema());
        if (hiveType != type) {
          LOG.warn("The given schema {} for the field {} does not match the schema {} from the table. " +
                     "The schema {} for field {} be used.", type, name, hiveType, hiveType, name);
        }
        fields.add(hCatFieldSchema);
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
   * @param schema {@link Schema} of the field
   * @return {@link PrimitiveTypeInfo} for the given {@link Schema.Type} which is compatible with Hive.
   */
  private static PrimitiveTypeInfo getType(String name, Schema schema) {
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    switch (type) {
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
          "Schema contains field '%s' with unsupported type %s. " +
            "You should provide an schema with this field dropped to work with this table.", name, type));
    }
  }

  /**
   * <p>Converts a {@link HCatSchema} from hive to {@link Schema} for CDAP.</p>
   * <p><b>Note:</b> This conversion does not support non-primitive types and the conversion will fail.
   * The conversion might also change the primitive type.
   * See {@link #getType(String, PrimitiveObjectInspector.PrimitiveCategory)} for details.</p>
   * The valid types of {@link HCatFieldSchema} which can be converted into {@link Schema} are boolean, byte, char,
   * short, int, long, float, double, string, varchar, binary
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
      fields.add(Schema.Field.of(name, getType(name, field.getTypeInfo().getPrimitiveCategory())));
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
  private static Schema getType(String name, PrimitiveObjectInspector.PrimitiveCategory category) {
    System.out.println("### primitive cat " + category);
    switch (category) {
      case BOOLEAN:
        return Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
      case BYTE:
      case SHORT:
      case INT:
        return Schema.nullableOf(Schema.of(Schema.Type.INT));
      case LONG:
        return Schema.nullableOf(Schema.of(Schema.Type.LONG));
      case FLOAT:
        return Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
      case DOUBLE:
        return Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
      case CHAR:
      case STRING:
      case VARCHAR:
        return Schema.nullableOf(Schema.of(Schema.Type.STRING));
      case BINARY:
        return Schema.nullableOf(Schema.of(Schema.Type.BYTES));
      // We can support VOID by having Schema type as null but HCatRecord does not support VOID and since we read
      // write through HCatSchema and HCatRecord we are not supporting VOID too for consistent behavior.
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
