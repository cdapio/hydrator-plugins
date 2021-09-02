package io.cdap.plugin.format.thrift.transform;


import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactTransformer {

  Logger LOG = LoggerFactory.getLogger(CompactTransformer.class);

  public StructuredRecord decode(byte[] thriftRecordBinary) throws TException {
    // Set up buffer for feeding into Thrift protocol

    LOG.info("Entering Decode Method");
    TTransport byteBuffer = new TMemoryBuffer(thriftRecordBinary.length);
    TProtocol thriftProtocol = new TCompactProtocol(byteBuffer);
    LOG.info("TProtocol: " + thriftProtocol);

    //This sets the record to be read
    LOG.info("Writing Record to Buffer");
    byteBuffer.write(thriftRecordBinary);

    StructuredRecord result = readMessage(thriftProtocol);

    LOG.info("test");
    return result;
  }

  public StructuredRecord.Builder convertParToStructuredRecord(TBase parsedAnonymizedRecord) {
    List<Schema.Field> fields = getParFields();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(Schema.recordOf("record", fields));

    for (_Fields field : ParsedAnonymizedRecord._Fields.values()) {

      if (parsedAnonymizedRecord.getFieldValue(field) != null) {
        recordBuilder.set(field.getFieldName(), parsedAnonymizedRecord.getFieldValue(field));
      } else {
        LOG.info("Value missing for Field " + field.getFieldName());
      }
    }

    return recordBuilder;
  }

  private StructuredRecord readMessage(TProtocol thriftProtocol) throws TException {
    LOG.info("Reading Message...");
    TMessage msg = thriftProtocol.readMessageBegin();
    LOG.info("Successfully Read TMessage: " + msg.toString());

    LOG.info("Converting TMessage to PAR via TProtocol...");
    ParsedAnonymizedRecord parsedAnonymizedRecord = new ParsedAnonymizedRecord();
    parsedAnonymizedRecord.read(thriftProtocol);

    LOG.info("Converting PAR to Structured Method...");
    StructuredRecord result = convertParToStructuredRecord(parsedAnonymizedRecord).build();
    LOG.info("Successfully Converted PAR to StructuredRecord: ");

    thriftProtocol.readMessageEnd();
    return result;
  }

  private List<Schema.Field> getParFields() {
    List<Schema.Field> fields = new ArrayList<>();

    for (_Fields f : ParsedAnonymizedRecord._Fields.values()) {
      fields.add(Schema.Field.of(f.getFieldName(), Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }

    return fields;
  }
}
