package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.bson.BSONObject;

import java.io.IOException;

/**
 *
 */
public class BSONConverter extends RecordConverter<BSONObject, StructuredRecord> {
  private final Schema schema;

  public BSONConverter(Schema schema) {
    this.schema = schema;
  }

  @Override
  StructuredRecord transform(BSONObject bsonObject) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), convertField(bsonObject.get(field.getName()), org.apache.avro.Schema.parseJson(field.getSchema().toString()));
    }
    return builder.build();
  }
}
