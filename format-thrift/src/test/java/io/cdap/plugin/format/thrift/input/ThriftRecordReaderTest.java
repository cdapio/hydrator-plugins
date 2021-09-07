package io.cdap.plugin.format.thrift.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.liveramp.types.bang.AnonymousIdentifier;
import com.liveramp.types.parc.FileMetadata;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import com.liveramp.types.parc.ParsedAnonymizedRecord._Fields;
import com.liveramp.types.parc.RecordMetadata;
import io.cdap.plugin.format.thrift.transform.CompactTransformer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftRecordReaderTest {

  @Mock
  private RecordReader<BytesWritable, NullWritable> delegate;
  @Mock
  private CompactTransformer transformer;

  @Test
  public void getCurrentValue_deserialisedCorrectly_PARisMatching() throws IOException, InterruptedException, TException {
    // GIVEN
    ParsedAnonymizedRecord stubbedPar = createPAR();

    TSerializer serializer = new TSerializer(new Factory());
    byte[] parBytes = serializer.serialize(stubbedPar);

    when(delegate.getCurrentKey()).thenReturn(new BytesWritable(parBytes));
    ArgumentCaptor<ParsedAnonymizedRecord> captor = ArgumentCaptor.forClass(ParsedAnonymizedRecord.class);

    // WHEN
    ThriftRecordReader thriftRecordReader = new ThriftRecordReader(delegate, transformer);
    thriftRecordReader.getCurrentValue();

    // THEN
    verify(transformer, times(1)).convertParToStructuredRecord(captor.capture());
    ParsedAnonymizedRecord deserialisedPar = captor.getValue();
    assertEquals(stubbedPar, deserialisedPar);
  }

  @Test
  public void getCurrentValue_deserialisedFailed_exceptionRaised() throws IOException, InterruptedException, TException {
    // GIVEN
    ParsedAnonymizedRecord stubbedPar = createPAR();

    TSerializer serializer = new TSerializer(new Factory());
    byte[] parBytes = serializer.serialize(stubbedPar);
    // Corrupting the serialised bytes
    parBytes[1] = '0';

    when(delegate.getCurrentKey()).thenReturn(new BytesWritable(parBytes));

    // WHEN
    ThriftRecordReader thriftRecordReader = new ThriftRecordReader(delegate, transformer);
    try {
      thriftRecordReader.getCurrentValue();
      fail("Should have raised an IOException");
    } catch (IOException ex) {
      assertTrue(ex.getMessage().startsWith("Error in getCurrentValue for key"));
    }

    // THEN
    verify(transformer, never()).convertParToStructuredRecord(any());
  }

  private ParsedAnonymizedRecord createPAR() {
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

    ParsedAnonymizedRecord par = new ParsedAnonymizedRecord();
    par.setFieldValue(_Fields.METADATA, recordMetadata);
    par.setFieldValue(_Fields.IDENTIFIERS, identifiers);
    par.setFieldValue(_Fields.KEY_TO_DATA, keyToData);
    par.setFieldValue(_Fields.KEY_TO_ANONYMIZED_STRIPPED_VALUES, keyToAnonStripe);
    par.setFieldValue(_Fields.LEGACY_AUDIENCE_KEY, "sadasd".getBytes(StandardCharsets.UTF_8));

    return par;
  }
}