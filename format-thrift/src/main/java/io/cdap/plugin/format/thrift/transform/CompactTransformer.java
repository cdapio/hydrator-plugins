package io.cdap.plugin.format.thrift.transform;


import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
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
        System.out.println("Entering Decode Method");
        TTransport byteBuffer = new TMemoryBuffer(thriftRecordBinary.length);
        setThriftProtocol(new TBinaryProtocol(byteBuffer));

        //This sets the record to be read
        LOG.debug("Writing Record to Buffer");
        System.out.println("Writing Record to Buffer");
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

        List<Schema.Field> fields = getParFields();

        StructuredRecord.Builder recordBuilder =  StructuredRecord.builder(Schema.recordOf("record", fields));

        TMessage msg = prot.readMessageBegin();

        for ( Schema.Field field : fields ){
            recordBuilder.set(field.getName(), msg.name);
        }

        prot.readMessageEnd();

        return recordBuilder.build();
    }

    private List<Schema.Field> getParFields() {
        List<Schema.Field> fields = new ArrayList<>();
        final String PEL = "PEL";
        final String IDENTIFIER_TYPE = "identifier_type";

        fields.add(Schema.Field.of(PEL, Schema.of(Schema.Type.STRING)));
        fields.add(Schema.Field.of(IDENTIFIER_TYPE, Schema.of(Schema.Type.STRING)));

        return fields;
    }

    public TProtocol getThriftProtocol() {
        return thriftProtocol;
    }
}
