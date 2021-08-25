package io.cdap.plugin.format.thrift.transform;

import com.liveramp.types.bang.AnonymousIdentifier;
import com.liveramp.types.parc.FileMetadata;
import com.liveramp.types.parc.NoMetadata;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import com.liveramp.types.parc.RecordMetadata;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class CompactTransformerTest {

  Logger LOG = LoggerFactory.getLogger(CompactTransformerTest.class);

  @Test
  public void decode_whenValidTCompactProvided_returnsStructureRecord() throws TException {
    LOG.debug("Starting test");
    CompactTransformer compactTransformer = new CompactTransformer();
    AutoExpandingBufferWriteTransport transportBuffer = new AutoExpandingBufferWriteTransport(32000, 1.5);

    writeParcFormattedTCompactProtocol(transportBuffer);

//    writeMsg(tProtocol, "test");

    // Flush and return the buffer
    transportBuffer.flush();
    transportBuffer.close();
    byte[] finalBuf = transportBuffer.getBuf().array();
    int resultSize = transportBuffer.getPos();

    // Put the output buffer here
    byte[] result = new byte[resultSize];
    System.arraycopy(finalBuf, 0, result, 0, resultSize);

    StructuredRecord bob = compactTransformer.decode(result);

    System.out.printf("end" + bob.toString());
  }


  public void writeParcFormattedTCompactProtocol(AutoExpandingBufferWriteTransport transportBuffer) throws TException {

    ParsedAnonymizedRecord par = new ParsedAnonymizedRecord();

    RecordMetadata recordMetadata = new RecordMetadata();
    FileMetadata fileMetadata = new FileMetadata(1L);
    recordMetadata.set_file_metadata(fileMetadata);

//    NoMetadata noMetadata = new NoMetadata();
    par.setFieldValue(_Fields.METADATA, recordMetadata);


    AnonymousIdentifier anonymousIdentifier = new AnonymousIdentifier();
    String number = "0123124124124";
    anonymousIdentifier.set_liveramp_mobile_id(number.getBytes(StandardCharsets.UTF_8));
    Set<AnonymousIdentifier> identifiers = new HashSet<>();
    identifiers.add(anonymousIdentifier);
    par.setFieldValue(_Fields.IDENTIFIERS, identifiers);


    Map<String,String> keyToData = new HashMap<>();
    keyToData.put("keyToData","Values");
    par.setFieldValue(_Fields.KEY_TO_DATA, keyToData);

    Map<String,String> keyToAnonStripe = new HashMap<>();
    keyToData.put("keyToAnonStripe","Stripes");
    par.setFieldValue(_Fields.KEY_TO_ANONYMIZED_STRIPPED_VALUES, keyToAnonStripe);

    TCompactProtocol protocol = new TCompactProtocol(transportBuffer);

    par.write(protocol);
    System.out.println("TEST");
  }

  private void writeMsg(TProtocol tProtocol, String input) throws TException {
    TMessage msg = new TMessage(
        "testMessageName",
        TType.STRING,
        1);

    tProtocol.writeMessageBegin(msg);

    writeFields(tProtocol, input);

    tProtocol.writeMessageEnd();
  }


  private void writeFields(TProtocol tProtocol, String input) throws TException {

    for (_Fields f : ParsedAnonymizedRecord._Fields.values()) {
      System.out.println(f.getFieldName());

      TField field = new TField(
          f.getFieldName(),
          TypeUtils.getTypeCode("string"),
          f.getThriftFieldId());

      tProtocol.writeFieldBegin(field);

      tProtocol.writeString(input + "-" + f.getFieldName());

      tProtocol.writeFieldEnd();

    }
    // Write stop
    tProtocol.writeByte((byte) 0);

  }

  private void writeField(TProtocol tProtocol, String input) throws TException {
    //Writing a single field
    TField field = new TField(
        "",
        TypeUtils.getTypeCode("string"),
        (short) 1);

    tProtocol.writeFieldBegin(field);

    tProtocol.writeString(input);

    tProtocol.writeFieldEnd();

    // Write stop
    tProtocol.writeByte((byte) 0);
  }
}
