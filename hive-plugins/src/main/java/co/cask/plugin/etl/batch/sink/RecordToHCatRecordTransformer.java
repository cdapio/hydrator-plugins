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
  private final Schema schema;

  public RecordToHCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  public HCatSchema gethCatSchema() {
    return hCatSchema;
  }



  public HCatRecord toHCatRecord(StructuredRecord record) throws HCatException {

    //TODO: Support writing dropped field schema
    Preconditions.checkArgument(schema.equals(record.getSchema()), "The schema of StructuredRecord being written does not match table's schema.");

    HCatRecord hCatRecord = new DefaultHCatRecord(record.getSchema().getFields().size());

    LOG.info("###the schema field size is {} ", record.getSchema().getFields().size());

    for (Schema.Field field : record.getSchema().getFields()) {
      hCatRecord.set(field.getName(), hCatSchema, record.get(field.getName()));
    }

    return hCatRecord;
  }
}