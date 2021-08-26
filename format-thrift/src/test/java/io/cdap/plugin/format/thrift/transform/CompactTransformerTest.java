package io.cdap.plugin.format.thrift.transform;

import com.liveramp.types.bang.AnonymousIdentifier;
import com.liveramp.types.parc.FileMetadata;
import com.liveramp.types.parc.NoMetadata;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import com.liveramp.types.parc.RecordMetadata;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

public class CompactTransformerTest {

  Logger LOG = LoggerFactory.getLogger(CompactTransformerTest.class);

  @Test
  public void decode_whenValidTCompactProvided_returnsStructureRecord() throws TException {
    //Given:
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
    CompactTransformer compactTransformer = new CompactTransformer();

    AutoExpandingBufferWriteTransport transportBuffer = writeMsg(par);

    // Flush and return the buffer
    transportBuffer.flush();
    transportBuffer.close();
    byte[] finalBuf = transportBuffer.getBuf().array();
    int resultSize = transportBuffer.getPos();

    // Put the output buffer here
    byte[] result = new byte[resultSize];
    System.arraycopy(finalBuf, 0, result, 0, resultSize);

    StructuredRecord output = compactTransformer.decode(result);

    //Assert Mandatory fields match input and present
    assertEquals(recordMetadata, output.get(_Fields.METADATA.getFieldName()));
    assertEquals(identifiers, output.get(_Fields.IDENTIFIERS.getFieldName()));
    assertEquals(keyToData, output.get(_Fields.KEY_TO_DATA.getFieldName()));
    assertEquals(keyToAnonStripe, output.get(_Fields.KEY_TO_ANONYMIZED_STRIPPED_VALUES.getFieldName()));

    //Assert Optional fields are null
    assertNull(output.get(_Fields.KEY_TO_IDENTIFIERS.getFieldName()));
    assertNull(output.get(_Fields.DATA_TOKENS.getFieldName()));
    assertNull(output.get(_Fields.KEY_TO_IDENTIFIERS_AND_METADATA.getFieldName()));
    assertNull(output.get(_Fields.FILEWIDE_ATTRIBUTE_ID_TO_DATA.getFieldName()));

    //Assert Optional field is not null when Set
    assertNotNull(output.get(_Fields.LEGACY_AUDIENCE_KEY.getFieldName()));
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
}
