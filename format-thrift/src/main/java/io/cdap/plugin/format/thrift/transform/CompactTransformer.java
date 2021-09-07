package io.cdap.plugin.format.thrift.transform;


import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactTransformer {

  Logger LOG = LoggerFactory.getLogger(CompactTransformer.class);

  public StructuredRecord.Builder convertParToStructuredRecord(TBase<ParsedAnonymizedRecord, ParsedAnonymizedRecord._Fields> parsedAnonymizedRecord) {
    List<Schema.Field> fields = getParFields();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(Schema.recordOf("record", fields));

    for (_Fields field : ParsedAnonymizedRecord._Fields.values()) {

      if (parsedAnonymizedRecord.getFieldValue(field) != null) {
        recordBuilder.set(field.getFieldName(), parsedAnonymizedRecord.getFieldValue(field));
      }
    }

    return recordBuilder;
  }

  private List<Schema.Field> getParFields() {
    List<Schema.Field> fields = new ArrayList<>();

    for (_Fields f : ParsedAnonymizedRecord._Fields.values()) {
      fields.add(Schema.Field.of(f.getFieldName(), Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }

    return fields;
  }
}
