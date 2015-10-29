package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by rsinha on 10/28/15.
 */
public class RecordToHCatRecordTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(RecordToHCatRecordTransformer.class);

  private final HCatSchema hCatSchema;

  public RecordToHCatRecordTransformer(HCatSchema hCatSchema) {
    this.hCatSchema = hCatSchema;
  }

  public HCatSchema gethCatSchema() {
    return hCatSchema;
  }

  private static HCatSchema convertSchema(Schema schema) {
    List<HCatFieldSchema> fields = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      try {
        fields.add(new HCatFieldSchema(name, getType(name, field.getSchema().getType()), null));
      } catch (HCatException e) {
        LOG.error("Failed to create HCatFieldSchema field {} of type {} from schema", name, field.getSchema().getType());
      }
    }
    return new HCatSchema(fields);
  }

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


  public HCatRecord toHCatRecord(StructuredRecord record) throws HCatException {

    //TODO: Support writing dropped field schema
    Preconditions.checkArgument(hCatSchema.equals(convertSchema(record.getSchema())), "The schema of StructuredRecord being written does not match table's schema.");

    HCatRecord hCatRecord = new DefaultHCatRecord(record.getSchema().getFields().size());

    for (Schema.Field field : record.getSchema().getFields()) {
      hCatRecord.set(field.getName(), hCatSchema, record.get(field.getName()));
    }

    return hCatRecord;
  }
}
