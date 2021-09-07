package io.cdap.plugin.format.thrift.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.liveramp.types.bang.AnonymousIdentifier;
import com.liveramp.types.parc.FileMetadata;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import com.liveramp.types.parc.RecordMetadata;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.StructuredRecord.Builder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.junit.Test;

public class CompactTransformerTest {

  @Test
  public void convertParToStructuredRecord_whenValidTCompactProvided_returnsStructureRecord() {
    // GIVEN
    RecordMetadata recordMetadata = new RecordMetadata();
    FileMetadata fileMetadata = new FileMetadata(1L);
    recordMetadata.set_file_metadata(fileMetadata);

    AnonymousIdentifier anonymousIdentifier = new AnonymousIdentifier();
    String number = "0123124124124";
    anonymousIdentifier.set_liveramp_mobile_id(number.getBytes(StandardCharsets.UTF_8));
    Set<AnonymousIdentifier> identifiers = new HashSet<>();
    identifiers.add(anonymousIdentifier);

    Map<String,String> keyToData = new HashMap<>();
    keyToData.put("keyToData","Values");

    Map<String,String> keyToAnonStripe = new HashMap<>();
    keyToAnonStripe.put("keyToAnonStripe","Stripes");

    ParsedAnonymizedRecord par = createPar(recordMetadata, identifiers, keyToData, keyToAnonStripe);
    par.setFieldValue(_Fields.LEGACY_AUDIENCE_KEY, "sadasd".getBytes(StandardCharsets.UTF_8));

    // WHEN
    CompactTransformer compactTransformer = new CompactTransformer();
    Builder builder = compactTransformer.convertParToStructuredRecord(par);

    // THEN
    StructuredRecord structuredRecord = builder.build();

    //Assert Mandatory fields match input and present
    assertEquals(recordMetadata, structuredRecord.get(_Fields.METADATA.getFieldName()));
    assertEquals(identifiers, structuredRecord.get(_Fields.IDENTIFIERS.getFieldName()));
    assertEquals(keyToData, structuredRecord.get(_Fields.KEY_TO_DATA.getFieldName()));
    assertEquals(keyToAnonStripe, structuredRecord.get(_Fields.KEY_TO_ANONYMIZED_STRIPPED_VALUES.getFieldName()));

    //Assert Optional fields are null
    assertNull(structuredRecord.get(_Fields.KEY_TO_IDENTIFIERS.getFieldName()));
    assertNull(structuredRecord.get(_Fields.DATA_TOKENS.getFieldName()));
    assertNull(structuredRecord.get(_Fields.KEY_TO_IDENTIFIERS_AND_METADATA.getFieldName()));
    assertNull(structuredRecord.get(_Fields.FILEWIDE_ATTRIBUTE_ID_TO_DATA.getFieldName()));

    //Assert Optional field is not null when Set
    assertNotNull(structuredRecord.get(_Fields.LEGACY_AUDIENCE_KEY.getFieldName()));
  }

  private byte[] trimArrayToBufferPosition(AutoExpandingBufferWriteTransport transportBuffer) {
    byte[] finalBuf = transportBuffer.getBuf().array();
    int resultSize = transportBuffer.getPos();

    // Put the output buffer here
    byte[] result = new byte[resultSize];
    System.arraycopy(finalBuf, 0, result, 0, resultSize);
    return result;
  }


  public ParsedAnonymizedRecord createPar(RecordMetadata recordMetadata, Set<AnonymousIdentifier> identifiers, Map<String,String> keyToData, Map<String,String> keyToAnonStripe ){
    ParsedAnonymizedRecord par = new ParsedAnonymizedRecord();
    par.setFieldValue(_Fields.METADATA, recordMetadata);
    par.setFieldValue(_Fields.IDENTIFIERS, identifiers);
    par.setFieldValue(_Fields.KEY_TO_DATA, keyToData);
    par.setFieldValue(_Fields.KEY_TO_ANONYMIZED_STRIPPED_VALUES, keyToAnonStripe);
    return par;
  }

  private AutoExpandingBufferWriteTransport writeMsg(ParsedAnonymizedRecord par) throws TException {
    AutoExpandingBufferWriteTransport transportBuffer = new AutoExpandingBufferWriteTransport(32000, 1.5);
    TMessage msg = new TMessage(
        "testMessageName",
        TType.STRING,
        1);

    TCompactProtocol protocol = new TCompactProtocol(transportBuffer);
    protocol.writeMessageBegin(msg);

    par.write(protocol);

    protocol.writeMessageEnd();
    return transportBuffer;
  }
//
//  private StructuredRecord decode(byte[] thriftRecordBinary) throws TException {
//    // Set up buffer for feeding into Thrift protocol
//
//    LOG.info("Entering Decode Method");
//    TTransport byteBuffer = new TMemoryBuffer(thriftRecordBinary.length);
//    TProtocol thriftProtocol = new TCompactProtocol(byteBuffer);
//    LOG.info("TProtocol: " + thriftProtocol);
//
//    //This sets the record to be read
//    LOG.info("Writing Record to Buffer");
//    byteBuffer.write(thriftRecordBinary);
//
//    StructuredRecord result = readMessage(thriftProtocol);
//
//    LOG.info("test");
//    return result;
//  }

}
