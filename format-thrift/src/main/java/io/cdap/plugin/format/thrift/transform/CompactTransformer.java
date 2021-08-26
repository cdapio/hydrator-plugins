package io.cdap.plugin.format.thrift.transform;


import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CompactTransformer {

    private TProtocol thriftProtocol;

    Logger LOG = LoggerFactory.getLogger(CompactTransformer.class);

    public StructuredRecord decode(byte[] thriftRecordBinary) throws TException {
        // Set up buffer for feeding into Thrift protocol

        LOG.debug("Entering Decode Method");
        TTransport byteBuffer = new TMemoryBuffer(thriftRecordBinary.length);
        setThriftProtocol(new TCompactProtocol(byteBuffer));
        LOG.debug("TProtocol: " + getThriftProtocol().toString());

        //This sets the record to be read
        LOG.debug("Writing Record to Buffer");
        byteBuffer.write(thriftRecordBinary);

        StructuredRecord result = readMessage();

        LOG.debug("test");
        return result;
    }

    protected void setThriftProtocol(TProtocol thriftProtocol) {
        this.thriftProtocol = thriftProtocol;
    }

    protected StructuredRecord readMessage () throws TException {
        TProtocol prot = getThriftProtocol();

        LOG.debug("Reading Message...");
        TMessage msg = prot.readMessageBegin();
        LOG.debug("Successfully Read TMessage: " + msg.toString());

        LOG.debug("Converting TMessage to PAR via TProtocol...");
        ParsedAnonymizedRecord parsedAnonymizedRecord = new ParsedAnonymizedRecord();
        parsedAnonymizedRecord.read(prot);

        LOG.debug("Converting PAR to Structured Method...");
        StructuredRecord result = convertParToStructuredRecord(parsedAnonymizedRecord);
        LOG.debug("Successfully Converted PAR to StructuredRecord: ");

        prot.readMessageEnd();

        return result;
    }

    private StructuredRecord convertParToStructuredRecord(ParsedAnonymizedRecord parsedAnonymizedRecord) {
        List<Schema.Field> fields = getParFields();
        StructuredRecord.Builder recordBuilder =  StructuredRecord.builder(Schema.recordOf("record", fields));

//        parsedAnonymizedRecord.

        for ( _Fields field : ParsedAnonymizedRecord._Fields.values() ){

           if (parsedAnonymizedRecord.getFieldValue(field) != null) {
               recordBuilder.set(field.getFieldName(), parsedAnonymizedRecord.getFieldValue(field));
           } else {
               LOG.debug("Value missing for Field " + field.getFieldName());
               System.out.println("Value missing for Field " + field.getFieldName());
//               recordBuilder.set(field.getFieldName(), "Test");
           }
        }

        return recordBuilder.build();
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
