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

    private TProtocol thriftProtocol;

    Logger LOG = LoggerFactory.getLogger(CompactTransformer.class);

    public StructuredRecord decode(byte[] thriftRecordBinary) throws TException {
        // Set up buffer for feeding into Thrift protocol

        LOG.info("Entering Decode Method");
        TTransport byteBuffer = new TMemoryBuffer(thriftRecordBinary.length);
        setThriftProtocol(new TCompactProtocol(byteBuffer));
        LOG.info("TProtocol: " + getThriftProtocol().toString());

        //This sets the record to be read
        LOG.info("Writing Record to Buffer");
        byteBuffer.write(thriftRecordBinary);

        StructuredRecord result = readMessage();

        LOG.info("test");
        return result;
    }

    protected void setThriftProtocol(TProtocol thriftProtocol) {
        this.thriftProtocol = thriftProtocol;
    }

    protected StructuredRecord readMessage () throws TException {
        TProtocol prot = getThriftProtocol();

        LOG.info("Reading Message...");
        TMessage msg = prot.readMessageBegin();
        LOG.info("Successfully Read TMessage: " + msg.toString());

        LOG.info("Converting TMessage to PAR via TProtocol...");
        ParsedAnonymizedRecord parsedAnonymizedRecord = new ParsedAnonymizedRecord();
        parsedAnonymizedRecord.read(prot);

        LOG.info("Converting PAR to Structured Method...");
        StructuredRecord result = convertParToStructuredRecord(parsedAnonymizedRecord).build();
        LOG.info("Successfully Converted PAR to StructuredRecord: ");

        prot.readMessageEnd();

        return result;
    }

    public StructuredRecord.Builder convertParToStructuredRecord(TBase parsedAnonymizedRecord) {
        List<Schema.Field> fields = getParFields();
        StructuredRecord.Builder recordBuilder =  StructuredRecord.builder(Schema.recordOf("record", fields));

        for ( _Fields field : ParsedAnonymizedRecord._Fields.values() ){

           if (parsedAnonymizedRecord.getFieldValue(field) != null) {
               recordBuilder.set(field.getFieldName(), parsedAnonymizedRecord.getFieldValue(field));
           } else {
               LOG.info("Value missing for Field " + field.getFieldName());
           }
        }

        return recordBuilder;
    }

    protected Object readFieldValue (byte fieldType) throws TException {
        TProtocol prot = getThriftProtocol();

        switch (fieldType) {
            case TType.BOOL:
                return prot.readBool();

            case TType.BYTE:
                return prot.readByte();

            case TType.DOUBLE:
                return prot.readDouble();

            case TType.I16:
                return prot.readI16();

            case TType.I32:
                return prot.readI32();

            case TType.I64:
                return prot.readI64();

            case TType.STRING:
                return prot.readString();

            case TType.STRUCT:
                return readStruct();

            case TType.MAP:
//                return readMap();

            case TType.SET:
//                return readSet();

            case TType.LIST:
//                return readList();

            case TType.ENUM:
                throw new UnsupportedOperationException("Enum ENUM reads not supported");

            case TType.VOID:
                throw new UnsupportedOperationException("Void type reads not supported");

            case TType.STOP:
                throw new IllegalArgumentException("Stop type has no value");

            default:
                throw new IllegalArgumentException("Unknown type with value " + (int) fieldType);
        }
    }

    protected ParsedAnonymizedRecord readStruct () throws TException {
        TProtocol prot = getThriftProtocol();

        prot.readStructBegin();
        //TODO figure out what structure needs to be read
        ParsedAnonymizedRecord record = new ParsedAnonymizedRecord();
        record.read(prot);
        prot.readStructEnd();

        return record;
    }

    private List<Schema.Field> getParFields() {
        List<Schema.Field> fields = new ArrayList<>();

        for (_Fields f: ParsedAnonymizedRecord._Fields.values()){
            fields.add(Schema.Field.of(f.getFieldName(), Schema.nullableOf(Schema.of(Schema.Type.STRING))));
        }

        return fields;
    }

    public TProtocol getThriftProtocol() {
        return thriftProtocol;
    }
}
