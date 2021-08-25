package io.cdap.plugin.format.thrift.transform;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.junit.Test;

public class CompactTransformerTest {

    @Test
    public void decode_whenValidTCompactProvided_returnsStructureRecord() throws TException {
        CompactTransformer compactTransformer = new CompactTransformer();
        AutoExpandingBufferWriteTransport transportBuffer = new AutoExpandingBufferWriteTransport(32000, 1.5);

        TProtocol tProtocol = new TBinaryProtocol(transportBuffer);

        writeMsg(tProtocol, "test");

        // Flush and return the buffer
        transportBuffer.flush();
        transportBuffer.close();
        byte[] finalBuf = transportBuffer.getBuf().array();
        int resultSize = transportBuffer.getPos();

        // Put the output buffer here
        byte[] result = new byte[resultSize];
        System.arraycopy(finalBuf, 0, result, 0, resultSize);

        compactTransformer.decode(result);
    }

    private void writeMsg(TProtocol tProtocol, String input) throws TException {
        TMessage msg = new TMessage("test",
                TType.STRING,
                1);

        tProtocol.writeMessageBegin(msg);

        writeField(tProtocol, input);

        tProtocol.writeMessageEnd();
    }

    private void writeField(TProtocol tProtocol, String input) throws TException {
        //Writing a single field
        TField field = new TField("",
                TypeUtils.getTypeCode("string"),
                        (short)1);

        tProtocol.writeFieldBegin(field);

        tProtocol.writeString(input);

        tProtocol.writeFieldEnd();

        // Write stop
        tProtocol.writeByte((byte) 0);
    }
}
