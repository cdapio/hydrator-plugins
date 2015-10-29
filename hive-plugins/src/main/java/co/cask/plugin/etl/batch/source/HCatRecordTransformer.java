package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;

/**
 * Created by rsinha on 10/28/15.
 */
public class HCatRecordTransformer {
  private final HCatSchema hCatSchema;
  private final Schema schema;

  public HCatRecordTransformer(HCatSchema hCatSchema) {
    this.hCatSchema = hCatSchema;
    this.schema = convertSchema(hCatSchema);
  }

  private static Schema convertSchema(HCatSchema hiveSchema) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (HCatFieldSchema field : hiveSchema.getFields()) {
      String name = field.getName();
      if (field.isComplex()) {
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with complex type %s. Only primitive types are supported.",
          name, field.getTypeString()));
      }
      PrimitiveTypeInfo typeInfo = field.getTypeInfo();
      fields.add(Schema.Field.of(name, Schema.of(getType(name, typeInfo.getPrimitiveCategory()))));
    }
    return Schema.recordOf("record", fields);
  }

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
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with unsupported type %s", name, category.name()));
    }
  }

  public StructuredRecord toRecord(HCatRecord hCatRecord) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      switch (field.getSchema().getType()) {
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
            throw new RuntimeException(String.format(
              "Error converting field '%s' of type %s", fieldName, field.getSchema().getType()), t);
          }
        }
        default: {
          throw new IllegalStateException(String.format(
            "Output schema contains field '%s' with unsupported type %s.",
            fieldName, field.getSchema().getType().name()));
        }
      }
    }

    return builder.build();
  }
}
